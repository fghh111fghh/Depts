from django.db import models
from django.core.validators import RegexValidator, MinValueValidator
from django.core.exceptions import ValidationError
from django.utils import timezone
from django.utils.text import slugify
from typing import Optional, Any
from unidecode import unidecode # Если используешь кириллицу
import time

# --- Валидаторы ---

def validate_website_domain(value: str) -> None:
    """
    Проверяет, что сайт заканчивается на допустимый домен.
    """
    allowed: tuple = ('.ru', '.com', '.net', '.org')
    if not any(value.lower().endswith(domain) for domain in allowed):
        raise ValidationError(f"Сайт должен заканчиваться на: {', '.join(allowed)}")


# --- Базовые классы (Abstract) ---

class BaseEntity(models.Model):
    """
    Абстрактная модель для всех сущностей системы.
    """
    name: models.CharField = models.CharField(
        max_length=100,
        unique=True,
        verbose_name='Наименование'
    )
    slug: models.SlugField = models.SlugField(
        max_length=100,
        db_index=True,
        unique=True,
        verbose_name="URL",
        blank=True
    )
    note: models.TextField = models.TextField(
        verbose_name='Примечание',
        blank=True
    )
    is_active: models.BooleanField = models.BooleanField(
        default=True,
        verbose_name='Активен'
    )
    time_create: models.DateTimeField = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Дата добавления"
    )
    time_update: models.DateTimeField = models.DateTimeField(
        auto_now=True,
        verbose_name="Дата обновления"
    )

    class Meta:
        abstract = True

    def __str__(self) -> str:
        return str(self.name)

    def save(self, *args: Any, **kwargs: Any) -> None:
        """Автоматическая генерация слага при сохранении."""
        if not self.slug:
            self.slug = slugify(self.name)
        super().save(*args, **kwargs)


class OrganizationBase(BaseEntity):
    """
    Абстрактная модель для организаций (СРО и Кредиторы).
    """
    website: models.URLField = models.URLField(
        max_length=100,
        unique=True,
        null=True,
        blank=True,
        verbose_name='Сайт',
        validators=[validate_website_domain],
        default='https://www.'
    )
    phone: models.CharField = models.CharField(
        max_length=20,
        unique=True,
        null=True,
        blank=True,
        verbose_name='Телефон',
        help_text="Формат: +7 (999) 000-00-00"
    )

    class Meta(BaseEntity.Meta):
        abstract = True

    def save(self, *args: Any, **kwargs: Any) -> None:
        """Корректировка URL: приведение к нижнему регистру и добавление префикса."""
        if self.website:
            self.website = self.website.strip().lower()
            if not self.website.startswith('https://www.'):
                clean_url = self.website.replace('https://', '').replace('http://', '').replace('www.', '')
                self.website = f'https://www.{clean_url}'
        super().save(*args, **kwargs)


# --- Реальные модели (Database Tables) ---

class SRO(OrganizationBase):
    """Саморегулируемая организация."""

    class Meta(OrganizationBase.Meta):
        verbose_name = 'СРО'
        verbose_name_plural = 'СРО'
        ordering = ['name']


class Creditor(OrganizationBase):
    """Кредитор (Банк, МФО)."""
    sro: models.ForeignKey = models.ForeignKey(
        SRO,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='creditors',
        verbose_name="Связь с СРО"
    )

    class Meta(OrganizationBase.Meta):
        verbose_name = 'Кредитор'
        verbose_name_plural = 'Кредиторы'
        ordering = ['name']


class Record(BaseEntity):
    """
    Запись о конкретной задолженности.
    """
    # Убираем общую уникальность имени, чтобы можно было использовать
    # повторяющиеся имена (например, 'Займ 1') у разных кредиторов.
    name: models.CharField = models.CharField(
        max_length=100,
        unique=False,
        verbose_name='Наименование'
    )

    creditor: models.ForeignKey = models.ForeignKey(
        Creditor,
        on_delete=models.PROTECT,  # Защита: нельзя удалить банк, пока есть долги
        related_name='records',
        verbose_name="Кредитор"
    )

    amount: models.DecimalField = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        verbose_name="Сумма долга",
        validators=[MinValueValidator(0.01)]
    )

    paid_amount: models.DecimalField = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        default=0,
        editable=False,
        verbose_name="Выплаченная сумма"
    )

    start_date: models.DateField = models.DateField(
        verbose_name="Дата кредита"
    )
    end_date: models.DateField = models.DateField(
        null=True,
        blank=True,
        verbose_name="Дата окончания кредита"
    )
    is_paid: models.BooleanField = models.BooleanField(
        default=False,
        verbose_name="Долг оплачен",
        db_index=True
    )

    class Meta(BaseEntity.Meta):
        verbose_name = 'Запись долга'
        verbose_name_plural = 'Записи долгов'
        ordering = ['end_date', 'amount']
        # Уникальность имени только в рамках одного кредитора
        unique_together = ('name', 'creditor')

    def clean(self) -> None:
        """Валидация бизнес-логики дат."""
        super().clean()
        if self.start_date and self.end_date:
            if self.end_date < self.start_date:
                raise ValidationError(
                    {'end_date': "Ошибка: дата окончания раньше даты начала."}
                )

        if self.start_date and self.start_date > timezone.now().date():
            raise ValidationError(
                {'start_date': "Ошибка: дата кредита не может быть в будущем."}
            )

    def save(self, *args: Any, **kwargs: Any) -> None:
        """Синхронизация выплаты и генерация уникального слага."""
        # 1. Логика 'Все или ничего'
        self.paid_amount = self.amount if self.is_paid else 0

        # 2. Уникальный слаг (имя + банк + дата + метка времени для исключения дублей)
        if not self.slug:
            # Используем unidecode, чтобы перевести "Запись 1" в "zapis-1"
            self.slug = slugify(unidecode(self.name))

        super().save(*args, **kwargs)

    def __str__(self) -> str:
        status: str = "✅ Оплачен" if self.is_paid else "❌ Активен"
        return f"{self.name} | {self.creditor.name} | {self.amount} ({status})"