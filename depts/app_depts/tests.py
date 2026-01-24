from django.test import TestCase
from django.core.exceptions import ValidationError
from django.utils import timezone
from decimal import Decimal
from datetime import timedelta
from django.db.models.deletion import ProtectedError
from .models import SRO, Creditor, Record, Transaction, TransactionType, CreditorType


class TotalDebtAnalysisTest(TestCase):

    def setUp(self):
        """Подготовка окружения: СРО, Кредиторы и базовый Долг"""
        self.sro = SRO.objects.create(name="Национальное СРО")
        self.bank = Creditor.objects.create(name="Альфа", creditor_type=CreditorType.BANK)
        self.mfo = Creditor.objects.create(name="ЗаймМигом", creditor_type=CreditorType.MFO, sro=self.sro)

        self.record = Record.objects.create(
            name="Кредит №1",
            creditor=self.bank,
            start_date=timezone.now().date() - timedelta(days=30)
        )

    # --- ГРУППА 1: ВАЛИДАЦИЯ И ЦЕЛОСТНОСТЬ БАЗЫ ---

    def test_mfo_requires_sro(self):
        """МФО не создается без СРО"""
        invalid_mfo = Creditor(name="Плохое МФО", creditor_type=CreditorType.MFO, sro=None)
        with self.assertRaises(ValidationError):
            invalid_mfo.full_clean()

    def test_creditor_protection(self):
        """Нельзя удалить банк, пока в нем есть долги (PROTECT)"""
        with self.assertRaises(ProtectedError):
            self.bank.delete()

    def test_prevent_creditor_change_with_history(self):
        """Запрет смены банка при наличии транзакций"""
        Transaction.objects.create(record=self.record, type=TransactionType.ACCRUAL, amount=Decimal('1000.00'))
        self.record.creditor = self.mfo
        with self.assertRaises(ValidationError):
            self.record.clean()

    # --- ГРУППА 2: ХРОНОЛОГИЯ И ДАТЫ ---

    def test_transaction_date_validations(self):
        """Транзакция не может быть в будущем или раньше открытия долга"""
        past_date = self.record.start_date - timedelta(days=1)
        tr_past = Transaction(record=self.record, type=TransactionType.ACCRUAL, amount=Decimal('500.00'),
                              date=past_date)
        with self.assertRaises(ValidationError):
            tr_past.full_clean()

        future_date = timezone.now().date() + timedelta(days=1)
        tr_future = Transaction(record=self.record, type=TransactionType.ACCRUAL, amount=Decimal('500.00'),
                                date=future_date)
        with self.assertRaises(ValidationError):
            tr_future.full_clean()

    # --- ГРУППА 3: МАТЕМАТИКА И ТОЧНОСТЬ (DECIMAL) ---

    def test_floating_point_precision(self):
        """Проверка точности: 100 платежей по 1.01 рубля"""
        Transaction.objects.create(record=self.record, type=TransactionType.ACCRUAL, amount=Decimal('100.00'))
        for _ in range(100):
            Transaction.objects.create(record=self.record, type=TransactionType.PAYMENT, amount=Decimal('1.01'))
        self.assertEqual(self.record.balance, Decimal('-1.00'))

    def test_zero_division_protection(self):
        """Защита от деления на ноль в расчете процентов"""
        Transaction.objects.create(record=self.record, type=TransactionType.PAYMENT, amount=Decimal('100.00'))
        self.assertEqual(self.record.progress_percent, 0)

    # --- ГРУППА 4: ЖИЗНЕННЫЙ ЦИКЛ СТАТУСА ---

    def test_auto_is_paid_full_cycle(self):
        """Тест: начисление -> оплата -> удаление -> списание"""
        Transaction.objects.create(record=self.record, type=TransactionType.ACCRUAL, amount=Decimal('500.00'))
        self.record.refresh_from_db()
        self.assertFalse(self.record.is_paid)

        p = Transaction.objects.create(record=self.record, type=TransactionType.PAYMENT, amount=Decimal('500.00'))
        self.record.refresh_from_db()
        self.assertTrue(self.record.is_paid)

        p.delete()
        self.record.refresh_from_db()
        self.assertFalse(self.record.is_paid)

        Transaction.objects.create(record=self.record, type=TransactionType.WRITE_OFF, amount=Decimal('500.00'))
        self.record.refresh_from_db()
        self.assertTrue(self.record.is_paid)

    def test_transaction_update_triggers_status(self):
        """Изменение суммы существующей транзакции обновляет статус Record"""
        Transaction.objects.create(record=self.record, type=TransactionType.ACCRUAL, amount=Decimal('1000.00'))
        pay = Transaction.objects.create(record=self.record, type=TransactionType.PAYMENT, amount=Decimal('1000.00'))

        self.record.refresh_from_db()
        self.assertTrue(self.record.is_paid)

        pay.amount = Decimal('999.99')
        pay.save()

        self.record.refresh_from_db()
        self.assertFalse(self.record.is_paid)

    # --- ГРУППА 5: УНИКАЛЬНОСТЬ И УДАЛЕНИЕ ---

    def test_slug_and_unique_together(self):
        """Проверка уникальности связки Имя+Кредитор"""
        duplicate = Record(name="Кредит №1", creditor=self.bank, start_date=timezone.now().date())
        with self.assertRaises(ValidationError):
            duplicate.full_clean()

        r2 = Record.objects.create(name="Кредит №1", creditor=self.mfo, start_date=timezone.now().date())
        self.assertNotEqual(self.record.slug, r2.slug)

    def test_cascade_delete_transactions(self):
        """При удалении долга удаляются и его транзакции"""
        tr = Transaction.objects.create(record=self.record, type=TransactionType.ACCRUAL, amount=Decimal('100.00'))
        tr_id = tr.id
        self.record.delete()
        self.assertFalse(Transaction.objects.filter(id=tr_id).exists())

    # --- ГРУППА 6: ПЕРЕПЛАТА И ГРАНИЦЫ ---

    def test_overpayment_logic(self):
        """Переплата оставляет статус 'Закрыт' и ограничивает прогресс 100%"""
        Transaction.objects.create(record=self.record, type=TransactionType.ACCRUAL, amount=Decimal('100.00'))
        Transaction.objects.create(record=self.record, type=TransactionType.PAYMENT, amount=Decimal('150.00'))
        self.record.refresh_from_db()
        self.assertTrue(self.record.is_paid)
        self.assertEqual(self.record.progress_percent, 100.0)

    def test_transaction_on_exact_start_date(self):
        """Транзакция в день открытия долга разрешена"""
        tr = Transaction(record=self.record, type=TransactionType.ACCRUAL, amount=Decimal('1.00'),
                         date=self.record.start_date)
        tr.full_clean()  # Не должно вызвать исключение

    # --- ГРУППА 7: ЭКСТРЕМАЛЬНЫЕ КЕЙСЫ И СИГНАЛЫ ---

    def test_full_correction_to_zero(self):
        """Отрицательная корректировка обнуляет баланс и закрывает долг"""
        Transaction.objects.create(record=self.record, type=TransactionType.ACCRUAL, amount=Decimal('500.00'))
        Transaction.objects.create(record=self.record, type=TransactionType.CORRECTION, amount=Decimal('-500.00'))
        self.record.refresh_from_db()
        self.assertEqual(self.record.balance, Decimal('0.00'))
        self.assertTrue(self.record.is_paid)

    def test_bulk_transaction_delete(self):
        """Массовое удаление транзакций (через QuerySet) сбрасывает статус (проверка сигналов)"""
        Transaction.objects.create(record=self.record, type=TransactionType.ACCRUAL, amount=Decimal('100.00'))
        Transaction.objects.create(record=self.record, type=TransactionType.PAYMENT, amount=Decimal('100.00'))

        self.record.refresh_from_db()
        self.assertTrue(self.record.is_paid)

        # Массовое удаление (не вызывает метод .delete() модели, но активирует сигнал)
        Transaction.objects.filter(record=self.record).delete()

        self.record.refresh_from_db()
        self.assertEqual(self.record.balance, Decimal('0.00'))
        self.assertFalse(self.record.is_paid)

    def test_precision_math_comparison(self):
        """Проверка точности при наличии остатка в 1 копейку"""
        Transaction.objects.create(record=self.record, type=TransactionType.ACCRUAL, amount=Decimal('10.00'))
        for _ in range(3):
            Transaction.objects.create(record=self.record, type=TransactionType.PAYMENT, amount=Decimal('3.33'))
        self.record.refresh_from_db()
        self.assertEqual(self.record.balance, Decimal('0.01'))
        self.assertFalse(self.record.is_paid)

    def test_multiple_accruals_sum(self):
        """Суммирование разных типов начислений"""
        Transaction.objects.create(record=self.record, type=TransactionType.ACCRUAL, amount=Decimal('1000.00'))
        Transaction.objects.create(record=self.record, type=TransactionType.INTEREST, amount=Decimal('50.00'))
        Transaction.objects.create(record=self.record, type=TransactionType.PENALTY, amount=Decimal('10.00'))
        self.assertEqual(self.record.total_accrued, Decimal('1060.00'))

    # --- ГРУППА 8: ГЛУБОКАЯ ЛОГИКА И КОРРЕКТИРОВКИ ---

    def test_slug_uniqueness_high_speed(self):
        """Проверка уникальности слагов при идентичных именах и быстрой записи"""
        # Имитируем ситуацию, когда время не изменилось (или очень близко)
        # Создаем запись вручную, чтобы проверить, не возникнет ли конфликта
        r1 = Record.objects.create(name="Тест", creditor=self.bank, start_date=timezone.now().date())
        r2 = Record.objects.create(name="Тест", creditor=self.mfo, start_date=timezone.now().date())

        self.assertNotEqual(r1.slug, r2.slug)
        self.assertTrue(r1.slug.startswith('test-alfa'))
        self.assertTrue(r2.slug.startswith('test-zaimmigom'))

    def test_correction_impact_on_balance(self):
        """Проверка влияния корректировок на начисления и выплаты"""
        # 1. Положительная корректировка (+500) — это как доп. начисление
        Transaction.objects.create(
            record=self.record, type=TransactionType.CORRECTION, amount=Decimal('500.00')
        )
        self.assertEqual(self.record.total_accrued, Decimal('500.00'))

        # 2. Отрицательная корректировка (-200) — это как списание (увеличивает total_paid)
        Transaction.objects.create(
            record=self.record, type=TransactionType.CORRECTION, amount=Decimal('-200.00')
        )
        self.assertEqual(self.record.total_paid, Decimal('200.00'))
        self.assertEqual(self.record.balance, Decimal('300.00'))

    def test_progress_percent_limits(self):
        """Проверка, что прогресс не выходит за рамки 0-100%"""
        # Начислили 100, выплатили 150 (переплата)
        Transaction.objects.create(record=self.record, type=TransactionType.ACCRUAL, amount=Decimal('100.00'))
        Transaction.objects.create(record=self.record, type=TransactionType.PAYMENT, amount=Decimal('150.00'))

        self.assertEqual(self.record.progress_percent, 100.0)