from django import forms
from .models import Bet, Bank
from decimal import Decimal


class BetForm(forms.ModelForm):
    """Форма для создания и редактирования ставок."""

    # Константы для выбора доли Келли
    KELLY_CHOICES = [
        ('0.25', '0.25)'),
        ('0.5', '0.5'),
        ('0.75', '0.75'),
        ('1.0', '1.0'),
    ]

    # Поле для выбора доли Келли
    fractional_kelly = forms.ChoiceField(
        choices=KELLY_CHOICES,
        required=False,
        initial='0.5',
        label='Доля Келли',
        widget=forms.Select(attrs={'class': 'form-control', 'id': 'id_fractional_kelly'})
    )

    # Явно определяем bank_before как CharField, чтобы получить строку и обработать вручную
    bank_before = forms.CharField(
        required=True,
        widget=forms.HiddenInput(attrs={'id': 'id_bank_before'}),
        label='Банк до ставки'
    )

    class Meta:
        model = Bet
        fields = [
            'match_time', 'home_team', 'away_team', 'league',
            'odds_over', 'odds_under', 'recommended_target', 'recommended_odds',
            'poisson_prob', 'actual_prob', 'ev', 'n_last_matches', 'interval',
            'stake', 'bank_before', 'result', 'settled_at', 'notes', 'fractional_kelly'
        ]

        # Константы для классов CSS
        DEFAULT_INPUT_CLASS = 'form-control'
        HIDDEN_CLASS = 'd-none'

        widgets = {
            # Текстовые поля (только для чтения)
            'match_time': forms.TextInput(attrs={
                'class': DEFAULT_INPUT_CLASS,
                'readonly': 'readonly'
            }),
            'interval': forms.TextInput(attrs={'class': DEFAULT_INPUT_CLASS}),
            'notes': forms.Textarea(attrs={'class': DEFAULT_INPUT_CLASS, 'rows': 3}),

            # Выпадающие списки
            'home_team': forms.Select(attrs={'class': DEFAULT_INPUT_CLASS}),
            'away_team': forms.Select(attrs={'class': DEFAULT_INPUT_CLASS}),
            'league': forms.Select(attrs={'class': DEFAULT_INPUT_CLASS}),
            'recommended_target': forms.Select(attrs={'class': DEFAULT_INPUT_CLASS}),
            'result': forms.Select(attrs={'class': DEFAULT_INPUT_CLASS}),

            # Числовые поля
            'recommended_odds': forms.NumberInput(attrs={
                'class': DEFAULT_INPUT_CLASS,
                'step': '0.01'
            }),
            'poisson_prob': forms.NumberInput(attrs={
                'class': DEFAULT_INPUT_CLASS,
                'step': '0.1'
            }),
            'actual_prob': forms.NumberInput(attrs={
                'class': DEFAULT_INPUT_CLASS,
                'step': '0.1'
            }),
            'ev': forms.NumberInput(attrs={
                'class': DEFAULT_INPUT_CLASS,
                'step': '0.1'
            }),
            'stake': forms.NumberInput(attrs={
                'class': DEFAULT_INPUT_CLASS,
                'step': '0.01',
                'id': 'id_stake'
            }),

            # Дата
            'settled_at': forms.DateInput(attrs={
                'class': DEFAULT_INPUT_CLASS,
                'type': 'date'
            }),

            # Скрытые поля
            'odds_over': forms.HiddenInput(attrs={'class': HIDDEN_CLASS}),
            'odds_under': forms.HiddenInput(attrs={'class': HIDDEN_CLASS}),
            'n_last_matches': forms.HiddenInput(attrs={'class': HIDDEN_CLASS}),
        }

        error_messages = {
            'result': {
                'required': 'Пожалуйста, выберите результат ставки.',
            },
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Настройка поля result
        self._setup_result_field()


        # Скрытие подписей для невидимых полей
        self._hide_field_labels()

    def _setup_result_field(self):
        """Настройка поля result: обязательное, без PENDING."""
        self.fields['result'].required = True
        self.fields['result'].choices = [
            (Bet.ResultChoices.WIN, Bet.ResultChoices.WIN.label),
            (Bet.ResultChoices.LOSS, Bet.ResultChoices.LOSS.label),
            (Bet.ResultChoices.REFUND, Bet.ResultChoices.REFUND.label),
        ]


    def _hide_field_labels(self):
        """Скрытие подписей для скрытых полей."""
        hidden_fields = ['odds_over', 'odds_under', 'n_last_matches']
        for field in hidden_fields:
            self.fields[field].label = ''

    def clean_bank_before(self):
        """Очищает значение bank_before и преобразует в Decimal."""
        value = self.cleaned_data.get('bank_before')
        if not value:
            raise forms.ValidationError('Банк до ставки обязателен.')

        # Очищаем от всех видов пробелов и заменяем запятую на точку
        cleaned = value.replace(' ', '').replace('\xa0', '').replace(',', '.')

        try:
            return Decimal(cleaned)
        except Exception:
            raise forms.ValidationError('Введите корректное число')

    def clean_stake(self):
        """Очищает значение stake и преобразует в Decimal."""
        value = self.cleaned_data.get('stake')
        if value is None:
            return value

        if isinstance(value, str):
            cleaned = value.replace(' ', '').replace('\xa0', '').replace(',', '.')
            try:
                return Decimal(cleaned)
            except ValueError:
                raise forms.ValidationError('Введите корректное число')

        return value

    def clean(self):
        """Валидация формы."""
        cleaned_data = super().clean()
        result = cleaned_data.get('result')

        if not result:
            raise forms.ValidationError('Результат ставки обязателен.')

        return cleaned_data


class BankAdjustmentForm(forms.Form):
    TRANSACTION_CHOICES = [
        ('DEPOSIT', 'Пополнение (+5000)'),
        ('WITHDRAWAL', 'Снятие (-5000)'),
        ('CUSTOM', 'Произвольная сумма'),
    ]

    transaction_type = forms.ChoiceField(choices=TRANSACTION_CHOICES, label='Тип операции')
    amount = forms.DecimalField(max_digits=10, decimal_places=2, required=False, label='Сумма')
    custom_amount = forms.DecimalField(max_digits=10, decimal_places=2, required=False, label='Произвольная сумма')
    description = forms.CharField(widget=forms.Textarea, required=False, label='Описание')

    def clean(self):
        cleaned_data = super().clean()
        trans_type = cleaned_data.get('transaction_type')
        amount = cleaned_data.get('amount')
        custom = cleaned_data.get('custom_amount')

        if trans_type == 'CUSTOM' and not custom:
            raise forms.ValidationError('Укажите произвольную сумму')

        if trans_type != 'CUSTOM' and not amount:
            raise forms.ValidationError('Выберите сумму')

        return cleaned_data