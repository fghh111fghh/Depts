import random
import time
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, List

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from app_depts.models import (
    SRO, Creditor, Record, Transaction,
    TransactionType, CreditorType, LoanType
)


class Command(BaseCommand):
    """
    Команда для заполнения базы данных тестовыми данными.

    Создает иерархию объектов: СРО -> Кредиторы -> Записи -> Транзакции.
    Автоматически очищает старые данные перед запуском.
    """

    help: str = 'Заполняет базу данных тестовыми данными без нарушения валидации'

    def handle(self, *args: Any, **kwargs: Any) -> None:
        """
        Основной метод выполнения команды.
        """
        self.stdout.write('--- Очистка базы данных ---')

        # Используем транзакцию для атомарности операции очистки и создания
        with transaction.atomic():
            self._clear_data()
            self._generate_data()

        self.stdout.write(self.style.SUCCESS('Успешно создано 12 записей и связанные транзакции!'))

    def _clear_data(self) -> None:
        """
        Удаляет старые записи из связанных таблиц.
        """
        Transaction.objects.all().delete()
        Record.objects.all().delete()
        Creditor.objects.all().delete()
        SRO.objects.all().delete()

    def _generate_data(self) -> None:
        """
        Генерирует новый набор тестовых данных.
        """
        self.stdout.write('--- Начало генерации данных ---')

        # 1. Создаем СРО
        sro: SRO = SRO.objects.create(
            name="Национальное содружество МФО",
            phone="+79001112233",
            website="https://sro-mfo.ru"
        )

        # 2. Создаем Кредиторов
        banks_list: List[str] = ["Сбербанк", "Т-Банк", "Альфа-Банк", "ВТБ", "Газпромбанк"]
        mfos_list: List[str] = ["Займер", "Екапуста", "Манимен", "Быстроденьги"]

        creditor_objs: List[Creditor] = []

        for name in banks_list:
            c = Creditor.objects.create(
                name=name,
                creditor_type=CreditorType.BANK,
                website=f"https://{name.lower().replace('-', '')}.ru"
            )
            creditor_objs.append(c)

        for name in mfos_list:
            c = Creditor.objects.create(
                name=name,
                creditor_type=CreditorType.MFO,
                sro=sro,
                website=f"https://{name.lower()}.ru"
            )
            creditor_objs.append(c)

        # 3. Создаем записи долгов
        loan_names: List[str] = ["Кредит наличными", "Потребительский", "Кредитная карта", "Микрозайм", "Рассрочка"]
        today: date = timezone.now().date()

        for _ in range(12):
            creditor: Creditor = random.choice(creditor_objs)
            start_date: date = today - timedelta(days=random.randint(30, 200))

            # Генерация уникального slug через имя и время (как в модели)
            record_name: str = f"{random.choice(loan_names)} №{random.randint(1000, 9999)}"

            record: Record = Record.objects.create(
                name=record_name,
                creditor=creditor,
                loan_type=random.choice(LoanType.choices)[0],
                start_date=start_date,
                end_date=start_date + timedelta(days=365) if random.random() > 0.3 else None,
                note="Тестовая запись создана автоматически"
            )

            self._generate_transactions_for_record(record, start_date, today)

    def _generate_transactions_for_record(self, record: Record, start_date: date, today: date) -> None:
        """
        Создает цепочку транзакций для конкретного долга.

        Args:
            record: Объект модели Record.
            start_date: Дата начала долгового обязательства.
            today: Текущая дата для ограничения выплат.
        """
        # А) Основное начисление
        base_amount: Decimal = Decimal(str(random.randint(5000, 150000)))
        Transaction.objects.create(
            record=record,
            type=TransactionType.ACCRUAL,
            amount=base_amount,
            date=start_date
        )

        # Б) Проценты
        interest_amount: Decimal = (base_amount * Decimal('0.1')).quantize(Decimal('0.00'))
        Transaction.objects.create(
            record=record,
            type=TransactionType.INTEREST,
            amount=interest_amount,
            date=start_date + timedelta(days=2)
        )

        # В) Платежи
        num_potential_payments: int = random.randint(1, 6)
        for p in range(num_potential_payments):
            payment_date: date = start_date + timedelta(days=30 * (p + 1))

            if payment_date >= today:
                break

            payment_amount: Decimal = Decimal(str(random.randint(1000, 8000)))
            Transaction.objects.create(
                record=record,
                type=TransactionType.PAYMENT,
                amount=payment_amount,
                date=payment_date,
                comment=f"Платеж №{p + 1}"
            )

        # Г) Штраф
        if random.random() > 0.8:
            Transaction.objects.create(
                record=record,
                type=TransactionType.PENALTY,
                amount=Decimal('500.00'),
                date=today - timedelta(days=5),
                comment="Просрочка платежа"
            )