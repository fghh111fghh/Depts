from django.db import models
from django.core.validators import MinValueValidator, RegexValidator
from django.core.exceptions import ValidationError
from django.utils import timezone
from django.utils.text import slugify
from unidecode import unidecode
from django.db.models import Sum, Q
from django.db.models.signals import post_delete
from django.dispatch import receiver
import time


# --- Справочники ---

class CreditorType(models.TextChoices):
    BANK = 'BANK', 'Банк'
    MFO = 'MFO', 'МФО'
    PERSON = 'PERSON', 'Частное лицо'
    OTHER = 'OTHER', 'Прочее'


class LoanType(models.TextChoices):
    PAYDAY = 'PAYDAY', 'Займ до зарплаты'
    CONSUMER = 'CONSUMER', 'Потребительский кредит'
    MORTGAGE = 'MORTGAGE', 'Ипотека'
    CARD = 'CARD', 'Кредитная карта'
    AUTO = 'AUTO', 'Автокредит'
    LINE = 'LINE', 'Кредитная линия'


class TransactionType(models.TextChoices):
    ACCRUAL = 'ACCRUAL', 'Начисление (тело долга)'
    PAYMENT = 'PAYMENT', 'Оплата'
    INTEREST = 'INTEREST', 'Проценты'
    PENALTY = 'PENALTY', 'Штраф/Пени'
    WRITE_OFF = 'WRITE_OFF', 'Списание'
    COLLECTION = 'COLLECTION', 'Передача коллекторам'
    CORRECTION = 'CORRECTION', 'Корректировка'


def validate_not_future(value):
    if value > timezone.now().date():
        raise ValidationError("Дата не может быть в будущем.")


# --- Модели ---

class BaseEntity(models.Model):
    name = models.CharField(max_length=100, verbose_name='Наименование')
    slug = models.SlugField(max_length=150, db_index=True, unique=True, verbose_name="URL", blank=True)
    note = models.TextField(verbose_name='Примечание', blank=True)
    time_create = models.DateTimeField(auto_now_add=True, verbose_name="Дата добавления")
    time_update = models.DateTimeField(auto_now=True, verbose_name="Дата обновления")

    class Meta:
        abstract = True

    def __str__(self):
        return self.name


class SRO(BaseEntity):
    class Meta:
        verbose_name = 'СРО'
        verbose_name_plural = 'СРО'


class Creditor(BaseEntity):
    creditor_type = models.CharField(max_length=10, choices=CreditorType.choices, default=CreditorType.BANK,
                                     verbose_name="Тип")
    sro = models.ForeignKey(SRO, on_delete=models.SET_NULL, null=True, blank=True, related_name='creditors',
                            verbose_name="СРО")
    phone = models.CharField(
        max_length=20, blank=True, null=True, verbose_name="Телефон",
        validators=[RegexValidator(r'^\+?1?\d{9,15}$', "Формат: +79991234567")]
    )

    class Meta:
        verbose_name = 'Кредитор'
        verbose_name_plural = 'Кредиторы'
        ordering = ['name']

    def clean(self):
        if self.creditor_type == CreditorType.MFO and not self.sro:
            raise ValidationError({'sro': "Для МФО необходимо указать СРО."})

    def save(self, *args, **kwargs):
        if self.creditor_type != CreditorType.MFO:
            self.sro = None
        if not self.slug:
            self.slug = slugify(unidecode(self.name))
        super().save(*args, **kwargs)


class Record(BaseEntity):
    creditor = models.ForeignKey(Creditor, on_delete=models.PROTECT, related_name='records', verbose_name="Кредитор")
    loan_type = models.CharField(max_length=15, choices=LoanType.choices, default=LoanType.CONSUMER, verbose_name="Тип")
    start_date = models.DateField(verbose_name="Дата открытия", validators=[validate_not_future])
    end_date = models.DateField(null=True, blank=True, verbose_name="Плановое закрытие")
    is_paid = models.BooleanField(default=False, verbose_name="Закрыт", db_index=True)

    class Meta:
        verbose_name = 'Запись долга'
        verbose_name_plural = 'Записи долгов'
        unique_together = ('name', 'creditor')

    def clean(self):
        if self.end_date and self.start_date and self.end_date < self.start_date:
            raise ValidationError({'end_date': "Дата закрытия не может быть раньше открытия."})

        if self.pk:
            orig = Record.objects.filter(pk=self.pk).first()
            if orig and orig.creditor != self.creditor and self.transactions.exists():
                raise ValidationError({'creditor': "Нельзя менять кредитора, если по долгу уже есть транзакции."})

            first_tr = self.transactions.order_by('date').first()
            if first_tr and self.start_date > first_tr.date:
                raise ValidationError({'start_date': f"Уже есть транзакции от {first_tr.date}."})

    def update_status(self):
        """Метод защищен от None при сравнении"""
        accrued = float(self.total_accrued or 0)
        balance = float(self.balance or 0)

        # Сравнение только с числами float, чтобы избежать TypeError
        new_status = accrued > 0 and balance <= 0

        if self.is_paid != new_status:
            Record.objects.filter(pk=self.pk).update(is_paid=new_status)
            self.is_paid = new_status

    def save(self, *args, **kwargs):
        self.full_clean()
        if not self.slug:
            base = slugify(unidecode(f"{self.name}-{self.creditor.name}"))
            self.slug = f"{base}-{int(time.time())}"

        super().save(*args, **kwargs)
        if self.pk:
            self.update_status()

    @property
    def total_accrued(self):
        accrual_types = [TransactionType.ACCRUAL, TransactionType.INTEREST, TransactionType.PENALTY]
        data = self.transactions.aggregate(
            base=Sum('amount', filter=Q(type__in=accrual_types)),
            corr=Sum('amount', filter=Q(type=TransactionType.CORRECTION, amount__gt=0))
        )
        # Применяем float() и or 0 ко всем слагаемым
        return round(float(data['base'] or 0) + float(data['corr'] or 0), 2)

    @property
    def total_paid(self):
        pay_types = [TransactionType.PAYMENT, TransactionType.WRITE_OFF]
        data = self.transactions.aggregate(
            base=Sum('amount', filter=Q(type__in=pay_types)),
            corr=Sum('amount', filter=Q(type=TransactionType.CORRECTION, amount__lt=0))
        )
        return round(float(data['base'] or 0) + abs(float(data['corr'] or 0)), 2)

    @property
    def balance(self):
        return round(float(self.total_accrued or 0) - float(self.total_paid or 0), 2)

    @property
    def progress_percent(self):
        accrued = float(self.total_accrued or 0)
        if accrued <= 0: return 0
        return round(min((float(self.total_paid or 0) / accrued) * 100, 100), 1)


class Transaction(models.Model):
    record = models.ForeignKey(Record, on_delete=models.CASCADE, related_name='transactions', verbose_name="Долг")
    type = models.CharField(max_length=20, choices=TransactionType.choices, verbose_name="Тип")
    amount = models.DecimalField(max_digits=12, decimal_places=2, verbose_name="Сумма")
    date = models.DateField(default=timezone.now, verbose_name="Дата", validators=[validate_not_future])
    comment = models.CharField(max_length=255, blank=True, verbose_name="Комментарий")

    class Meta:
        verbose_name = 'Транзакция'
        verbose_name_plural = 'Транзакции'
        ordering = ['-date', '-id']

    def clean(self):
        # Ошибка со скриншота исправлена здесь: добавлена проверка на None перед сравнением <= 0
        current_amount = self.amount or 0

        if self.type != TransactionType.CORRECTION and current_amount <= 0:
            raise ValidationError({'amount': "Сумма должна быть положительной."})

        if self.record_id and self.date < self.record.start_date:
            raise ValidationError({'date': "Транзакция не может быть раньше открытия долга."})

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)
        if self.record:
            self.record.update_status()

    def __str__(self):
        return f"{self.date} | {self.get_type_display()} | {self.amount} р."


# --- Сигналы ---

@receiver(post_delete, sender=Transaction)
def auto_update_record_status(sender, instance, **kwargs):
    if instance.record:
        instance.record.update_status()