import random
from decimal import Decimal
from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from app_depts.models import SRO, Creditor, Record, Transaction, TransactionType, CreditorType, LoanType


class Command(BaseCommand):
    help = 'Заполняет базу данных тестовыми данными без нарушения валидации'

    def handle(self, *args, **kwargs):
        self.stdout.write('--- Очистка базы данных ---')
        # Удаляем старые записи, чтобы не плодить дубли
        Record.objects.all().delete()
        Creditor.objects.all().delete()
        SRO.objects.all().delete()

        self.stdout.write('--- Начало генерации данных ---')

        # 1. Создаем СРО
        sro = SRO.objects.create(name="Национальное содружество МФО")

        # 2. Создаем Кредиторов
        banks_list = ["Сбербанк", "Т-Банк", "Альфа-Банк", "ВТБ", "Газпромбанк"]
        mfos_list = ["Займер", "Екапуста", "Манимен", "Быстроденьги"]

        creditor_objs = []
        for name in banks_list:
            c = Creditor.objects.create(name=name, creditor_type=CreditorType.BANK)
            creditor_objs.append(c)

        for name in mfos_list:
            c = Creditor.objects.create(name=name, creditor_type=CreditorType.MFO, sro=sro)
            creditor_objs.append(c)

        # 3. Создаем записи долгов
        loan_names = ["Кредит наличными", "Потребительский", "Кредитная карта", "Микрозайм", "Рассрочка"]
        today = timezone.now().date()

        for i in range(12):  # Создадим 12 различных долгов
            creditor = random.choice(creditor_objs)
            # Старт долга от 200 до 30 дней назад
            start_date = today - timedelta(days=random.randint(30, 200))

            record = Record.objects.create(
                name=f"{random.choice(loan_names)} №{random.randint(1000, 9999)}",
                creditor=creditor,
                loan_type=random.choice(LoanType.choices)[0],
                start_date=start_date,
                # Плановое закрытие через год после старта (может быть пустым)
                end_date=start_date + timedelta(days=365) if random.random() > 0.3 else None
            )

            # --- Генерация транзакций ---

            # А) Основное начисление (тело долга) - всегда в дату старта
            base_amount = Decimal(str(random.randint(5000, 150000)))
            Transaction.objects.create(
                record=record,
                type=TransactionType.ACCRUAL,
                amount=base_amount,
                date=start_date
            )

            # Б) Проценты (через пару дней после старта)
            interest_amount = (base_amount * Decimal('0.1')).quantize(Decimal('0.00'))
            Transaction.objects.create(
                record=record,
                type=TransactionType.INTEREST,
                amount=interest_amount,
                date=start_date + timedelta(days=2)
            )

            # В) Платежи (раз в месяц)
            num_potential_payments = random.randint(1, 6)
            for p in range(num_potential_payments):
                # Дата каждого следующего платежа +30 дней
                payment_date = start_date + timedelta(days=30 * (p + 1))

                # ЖЕСТКАЯ ПРОВЕРКА: не создаем платеж, если его дата в будущем
                if payment_date >= today:
                    break

                # Сумма платежа (случайная, чтобы не всегда закрывала в ноль)
                payment_amount = Decimal(str(random.randint(1000, 8000)))

                Transaction.objects.create(
                    record=record,
                    type=TransactionType.PAYMENT,
                    amount=payment_amount,
                    date=payment_date
                )

            # Г) Штраф (редкое событие)
            if random.random() > 0.8:
                Transaction.objects.create(
                    record=record,
                    type=TransactionType.PENALTY,
                    amount=Decimal('500.00'),
                    date=today - timedelta(days=5)
                )

        self.stdout.write(self.style.SUCCESS(f'Успешно создано 12 записей и связанные транзакции!'))