document.addEventListener('DOMContentLoaded', function() {
    const oddsInput = document.getElementById('odds');
    const probInput = document.getElementById('probability');
    const bankInput = document.getElementById('bank');
    const kellySelect = document.getElementById('kelly_fraction');
    const calculateBtn = document.getElementById('calculateBtn');
    const defaultProbHint = document.getElementById('defaultProbHint');
    const useDefaultProbBtn = document.getElementById('useDefaultProb');

    const resultBlock = document.getElementById('resultBlock');
    const errorBlock = document.getElementById('errorBlock');
    const errorMessage = document.getElementById('errorMessage');

    // Желаемый ROI по умолчанию (5%)
    const DEFAULT_ROI = 5;

    function calculateDefaultProbability() {
        const odds = parseFloat(oddsInput.value);
        if (!odds || odds < 1.01) return null;

        // ROI = (p * odds - 1) * 100%
        // p * odds - 1 = ROI / 100
        // p * odds = 1 + ROI/100
        // p = (1 + ROI/100) / odds

        const p = (1 + DEFAULT_ROI / 100) / odds;
        const probPercent = p * 100;

        return Math.min(probPercent, 99.9); // Ограничиваем 99.9%
    }

    function updateDefaultProbability() {
        const defaultProb = calculateDefaultProbability();
        if (defaultProb) {
            defaultProbHint.textContent = `(для ROI ${DEFAULT_ROI}% нужно ${defaultProb.toFixed(1)}%)`;

            // Устанавливаем значение по умолчанию, только если поле пустое
            if (!probInput.value) {
                probInput.value = defaultProb.toFixed(1);
            }
        } else {
            defaultProbHint.textContent = '';
        }
    }

    function setDefaultProbability() {
        const defaultProb = calculateDefaultProbability();
        if (defaultProb) {
            probInput.value = defaultProb.toFixed(1);
        }
    }

    function formatNumber(value) {
        return new Intl.NumberFormat('ru-RU').format(Math.round(value));
    }

    function calculate() {
        const odds = parseFloat(oddsInput.value);
        let prob = parseFloat(probInput.value);
        const bank = parseFloat(bankInput.value);
        const fraction = parseFloat(kellySelect.value);

        if (!odds || odds < 1.01) {
            showError('Коэффициент должен быть больше 1.01');
            return false;
        }

        // Если вероятность не введена, используем расчетную
        if (!prob || prob <= 0) {
            const defaultProb = calculateDefaultProbability();
            if (defaultProb) {
                prob = defaultProb;
                probInput.value = defaultProb.toFixed(1);
            } else {
                showError('Невозможно рассчитать вероятность');
                return false;
            }
        }

        if (prob <= 0 || prob >= 100) {
            showError('Вероятность должна быть от 0.1% до 99.9%');
            return false;
        }

        if (!bank || bank < 100) {
            showError('Банк должен быть не менее 100');
            return false;
        }

        hideError();

        const p = prob / 100;
        const fullKelly = (p * odds - 1) / (odds - 1);
        const limitedKelly = Math.max(0, Math.min(fullKelly, 1));
        const stakeKelly = limitedKelly * fraction;
        const stake = bank * stakeKelly;
        const roundedStake = Math.round(stake / 100) * 100;
        const stakePercent = (stakeKelly * 100).toFixed(2);

        // Расчет текущего ROI
        const currentRoi = (p * odds - 1) * 100;

        document.getElementById('resultProb').textContent = prob.toFixed(1) + '%';
        document.getElementById('resultOdds').textContent = odds.toFixed(2);
        document.getElementById('resultBank').textContent = formatNumber(bank) + ' ₽';
        document.getElementById('resultKelly').textContent = fraction;

        // Добавляем индикатор ROI
        const roiIndicator = document.getElementById('roiIndicator') || document.createElement('div');
        roiIndicator.id = 'roiIndicator';
        roiIndicator.className = `roi-indicator ${currentRoi > 0 ? 'positive' : 'negative'}`;
        roiIndicator.innerHTML = `Текущий ROI: ${currentRoi > 0 ? '+' : ''}${currentRoi.toFixed(1)}%`;

        const resultGrid = document.querySelector('.result-grid');
        if (!document.getElementById('roiIndicator')) {
            resultGrid.appendChild(roiIndicator);
        } else {
            roiIndicator.innerHTML = `Текущий ROI: ${currentRoi > 0 ? '+' : ''}${currentRoi.toFixed(1)}%`;
            roiIndicator.className = `roi-indicator ${currentRoi > 0 ? 'positive' : 'negative'}`;
        }

        document.getElementById('recommendedStake').textContent = formatNumber(roundedStake) + ' ₽';
        document.getElementById('stakePercent').textContent = stakePercent + '%';

        const progressPercent = document.getElementById('progressPercent');
        const progressFill = document.getElementById('progressFill');
        const progressWidth = Math.min(stakeKelly * 100, 100);
        progressPercent.textContent = progressWidth.toFixed(1) + '%';
        progressFill.style.width = progressWidth + '%';

        resultBlock.style.display = 'block';

        return true;
    }

    function showError(message) {
        errorMessage.textContent = message;
        errorBlock.style.display = 'flex';
        resultBlock.style.display = 'none';
    }

    function hideError() {
        errorBlock.style.display = 'none';
    }

    // Обновляем подсказку при изменении коэффициента
    oddsInput.addEventListener('input', updateDefaultProbability);

    // Кнопка сброса к расчетной вероятности
    useDefaultProbBtn.addEventListener('click', setDefaultProbability);

    calculateBtn.addEventListener('click', calculate);

    [oddsInput, probInput, bankInput, kellySelect].forEach(input => {
        input.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                e.preventDefault();
                calculate();
            }
        });
    });

    document.querySelectorAll('.example-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const card = this.closest('.example-card');
            oddsInput.value = card.dataset.odds;
            probInput.value = card.dataset.prob;
            bankInput.value = card.dataset.bank;

            for (let option of kellySelect.options) {
                if (parseFloat(option.value) === parseFloat(card.dataset.kelly)) {
                    option.selected = true;
                    break;
                }
            }

            updateDefaultProbability();
            calculate();
        });
    });

    // Первоначальный расчет подсказки
    updateDefaultProbability();
});