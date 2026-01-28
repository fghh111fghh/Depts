from decimal import Decimal

from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator
from django.db import models
from django.db.models import Q, Sum
from django.db.models.functions import Coalesce
from django.db.models.signals import post_delete
from django.dispatch import receiver
from django.urls import reverse_lazy
from django.utils import timezone
from django.utils.text import slugify
from unidecode import unidecode


# --- Справочники ---

class CreditorType(models.TextChoices):
    """Типы организаций-кредиторов."""
    BANK = 'BANK', 'Банк'
    MFO = 'MFO', 'МФО'
    PERSON = 'PERSON', 'Частное лицо'
    OTHER = 'OTHER', 'Прочее'


class LoanType(models.TextChoices):
    """Категории долговых обязательств."""
    PAYDAY = 'PAYDAY', 'Займ до зарплаты'
    CONSUMER = 'CONSUMER', 'Потребительский кредит'
    MORTGAGE = 'MORTGAGE', 'Ипотека'
    CARD = 'CARD', 'Кредитная карта'
    AUTO = 'AUTO', 'Автокредит'
    LINE = 'LINE', 'Кредитная линия'


class TransactionType(models.TextChoices):
    """Типы финансовых операций."""
    ACCRUAL = 'ACCRUAL', 'Начисление (тело долга)'
    PAYMENT = 'PAYMENT', 'Оплата'
    INTEREST = 'INTEREST', 'Проценты'
    PENALTY = 'PENALTY', 'Штраф/Пени'
    WRITE_OFF = 'WRITE_OFF', 'Списание'
    COLLECTION = 'COLLECTION', 'Передача коллекторам'
    CORRECTION = 'CORRECTION', 'Корректировка'


def validate_not_future(value: timezone.now) -> None:
    """Запрещает выбор даты из будущего."""
    if value > timezone.now().date():
        raise ValidationError("Дата не может быть в будущем.")


# --- Абстрактные модели ---

class BaseEntity(models.Model):
    """Базовый абстрактный класс с общими метаданными."""
    name = models.CharField(max_length=100, verbose_name='Наименование')
    slug = models.SlugField(max_length=150, unique=True, db_index=True, verbose_name="URL", blank=True)
    note = models.TextField(verbose_name='Примечание', blank=True)
    time_create = models.DateTimeField(auto_now_add=True, verbose_name="Дата добавления")
    time_update = models.DateTimeField(auto_now=True, verbose_name="Дата обновления")

    class Meta:
        abstract = True

    def __str__(self) -> str:
        return self.name


class Organization(BaseEntity):
    """Абстрактное расширение для СРО и Кредиторов с контактами."""
    phone = models.CharField(
        max_length=20, blank=True, null=True, verbose_name="Телефон",
        validators=[RegexValidator(r'^\+?1?\d{9,15}$', "Формат: +79991234567")]
    )
    website = models.URLField(blank=True, null=True, verbose_name="Сайт")

    class Meta:
        abstract = True


# --- Основные модели ---

class SRO(Organization):
    """Саморегулируемая организация."""
    class Meta:
        verbose_name = 'СРО'
        verbose_name_plural = 'СРО'


class Creditor(Organization):
    """Организация или лицо, предоставившее займ."""
    creditor_type = models.CharField(
        max_length=10, choices=CreditorType.choices,
        default=CreditorType.BANK, verbose_name="Тип"
    )
    sro = models.ForeignKey(
        SRO, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='creditors', verbose_name="СРО"
    )

    class Meta:
        verbose_name = 'Кредитор'
        verbose_name_plural = 'Кредиторы'
        ordering = ['name']

    def clean(self) -> None:
        """Валидация обязательного наличия СРО для МФО."""
        if self.creditor_type == CreditorType.MFO and not self.sro:
            raise ValidationError({'sro': "Для МФО необходимо указать СРО."})

    def save(self, *args, **kwargs) -> None:
        """Автоматическая генерация slug и сброс СРО для не-МФО."""
        if self.creditor_type != CreditorType.MFO:
            self.sro = None
        if not self.slug:
            self.slug = slugify(unidecode(self.name))
        super().save(*args, **kwargs)


class Record(BaseEntity):
    """Запись о конкретном долге."""
    creditor = models.ForeignKey(
        Creditor, on_delete=models.PROTECT,
        related_name='records', verbose_name="Кредитор"
    )
    loan_type = models.CharField(
        max_length=15, choices=LoanType.choices,
        default=LoanType.CONSUMER, verbose_name="Тип"
    )
    start_date = models.DateField(verbose_name="Дата открытия", validators=[validate_not_future])
    end_date = models.DateField(null=True, blank=True, verbose_name="Плановое закрытие")
    is_paid = models.BooleanField(default=False, verbose_name="Закрыт", db_index=True)

    class Meta:
        verbose_name = 'Запись долга'
        verbose_name_plural = 'Записи долгов'
        unique_together = ('name', 'creditor')

    def get_absolute_url(self):
        return reverse_lazy('app_depts:records_detail', kwargs={'slug': self.slug})

    def clean(self) -> None:
        """Комплексная проверка дат и ограничений на смену кредитора."""
        if self.end_date and self.start_date and self.end_date < self.start_date:
            raise ValidationError({'end_date': "Дата закрытия не может быть раньше открытия."})

        if self.pk:
            orig = Record.objects.filter(pk=self.pk).first()
            if orig and orig.creditor != self.creditor and self.transactions.exists():
                raise ValidationError({'creditor': "Нельзя менять кредитора при наличии транзакций."})

            first_tr = self.transactions.order_by('date').first()
            if first_tr and self.start_date > first_tr.date:
                raise ValidationError({'start_date': f"Уже есть транзакции от {first_tr.date}."})

    def update_status(self) -> None:
        """Обновляет флаг is_paid на основе баланса."""
        accrued = self.total_accrued
        balance = self.balance
        new_status = accrued > 0 and balance <= 0

        if self.is_paid != new_status:
            Record.objects.filter(pk=self.pk).update(is_paid=new_status)
            self.is_paid = new_status

    def save(self, *args, **kwargs) -> None:
        """Генерация уникального slug и запуск валидации."""
        self.full_clean()
        if not self.slug:
            base = slugify(unidecode(f"{self.name}-{self.creditor.name}"))
            self.slug = f"{base}-{int(timezone.now().timestamp())}"

        super().save(*args, **kwargs)
        if self.pk:
            self.update_status()

    @property
    def total_accrued(self) -> Decimal:
        """Сумма всех начислений по долгу."""
        accrual_types = [TransactionType.ACCRUAL, TransactionType.INTEREST, TransactionType.PENALTY]
        data = self.transactions.aggregate(
            total=Coalesce(Sum('amount', filter=Q(type__in=accrual_types)), Decimal('0.00')),
            corr=Coalesce(Sum('amount', filter=Q(type=TransactionType.CORRECTION, amount__gt=0)), Decimal('0.00'))
        )
        return (data['total'] + data['corr']).quantize(Decimal('0.00'))

    @property
    def total_paid(self) -> Decimal:
        """Сумма всех выплат и списаний."""
        pay_types = [TransactionType.PAYMENT, TransactionType.WRITE_OFF]
        data = self.transactions.aggregate(
            total=Coalesce(Sum('amount', filter=Q(type__in=pay_types)), Decimal('0.00')),
            corr=Coalesce(Sum('amount', filter=Q(type=TransactionType.CORRECTION, amount__lt=0)), Decimal('0.00'))
        )
        return (data['total'] + abs(data['corr'])).quantize(Decimal('0.00'))

    @property
    def balance(self) -> Decimal:
        """Текущий остаток задолженности."""
        return self.total_accrued - self.total_paid

    @property
    def progress_percent(self) -> float:
        """Процент погашения (от 0 до 100)."""
        accrued = self.total_accrued
        if accrued <= 0:
            return 0.0
        percent = (float(self.total_paid) / float(accrued)) * 100
        return round(min(percent, 100.0), 1)


class Transaction(models.Model):
    """Финансовая операция по конкретному долгу."""
    record = models.ForeignKey(
        Record, on_delete=models.CASCADE,
        related_name='transactions', verbose_name="Долг"
    )
    type = models.CharField(max_length=20, choices=TransactionType.choices, verbose_name="Тип")
    amount = models.DecimalField(max_digits=12, decimal_places=2, verbose_name="Сумма")
    date = models.DateField(default=timezone.now, verbose_name="Дата", validators=[validate_not_future])
    comment = models.CharField(max_length=255, blank=True, verbose_name="Комментарий")

    class Meta:
        verbose_name = 'Транзакция'
        verbose_name_plural = 'Транзакции'
        ordering = ['-date', '-id']

    def clean(self) -> None:
        """Проверка корректности суммы и даты транзакции."""
        current_amount = self.amount or Decimal('0.00')
        if self.type != TransactionType.CORRECTION and current_amount <= 0:
            raise ValidationError({'amount': "Сумма должна быть положительной."})
        if self.record_id and self.date < self.record.start_date:
            raise ValidationError({'date': "Транзакция не может быть раньше открытия долга."})

    def save(self, *args, **kwargs) -> None:
        """Сохранение с последующим пересчетом статуса записи."""
        self.full_clean()
        super().save(*args, **kwargs)
        if self.record:
            self.record.update_status()

    def __str__(self) -> str:
        return f"{self.date} | {self.get_type_display()} | {self.amount} р."


# --- Сигналы ---

@receiver(post_delete, sender=Transaction)
def auto_update_record_status(sender, instance: Transaction, **kwargs) -> None:
    """Обновляет статус долга при удалении транзакции."""
    if instance.record:
        instance.record.update_status()