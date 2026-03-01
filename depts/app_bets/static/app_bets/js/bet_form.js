document.addEventListener('DOMContentLoaded', function() {
        const bankInput = document.getElementById('id_bank_before');
        const oddsInput = document.getElementById('id_recommended_odds');
        const probInput = document.getElementById('id_actual_prob');
        const kellySelect = document.getElementById('id_fractional_kelly');
        const stakeInput = document.getElementById('id_stake');
        const stakeDisplay = document.getElementById('id_stake_display');

        function getFloatValue(element) {
            if (!element || !element.value) return 0;
            return parseFloat(element.value.toString().replace(/\s/g, '').replace(',', '.')) || 0;
        }

        function roundToHundreds(value) {
            return Math.round(value / 100) * 100;
        }

        function formatNumberWithSpaces(value) {
            return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, " ");
        }

        function calculateStake() {
        const bank = getFloatValue(bankInput);
        const odds = getFloatValue(oddsInput);
        const prob = getFloatValue(probInput);
        const fraction = parseFloat(kellySelect.value) || 0;

        if (odds > 1 && prob > 0 && prob <= 100 && bank > 0) {
            // ПРЕДПОЛАГАЕМ, ЧТО prob В ПРОЦЕНТАХ (0-100)
            const p = prob / 100;  // переводим в доли

            // Критерий Келли: f = (p * k - 1) / (k - 1)
            const fullKelly = (p * odds - 1) / (odds - 1);

            // Ограничиваем от 0 до 1
            const limitedKelly = Math.max(0, Math.min(fullKelly, 1));

            // Сумма ставки
            const stake = bank * limitedKelly * fraction;

            // Округляем до сотен
            const roundedStake = roundToHundreds(stake);

            // Если пользователь не менял сумму вручную
            if (!stakeDisplay._userModified) {
                stakeInput.value = roundedStake;
                stakeDisplay.value = formatNumberWithSpaces(roundedStake);
                console.log('СТАВКА УСТАНОВЛЕНА АВТОМАТИЧЕСКИ');
            } else {
                console.log('СТАВКА НЕ ИЗМЕНЕНА (пользователь ввел вручную)');
            }
        } else {
            console.log('УСЛОВИЕ НЕ ВЫПОЛНЕНО:');
            if (odds <= 1) console.log('- odds <= 1');
            if (prob <= 0) console.log('- prob <= 0');
            if (prob >= 100) console.log('- prob >= 100');
            if (bank <= 0) console.log('- bank <= 0');

            if (!stakeDisplay._userModified) {
                stakeInput.value = 0;
                stakeDisplay.value = '0';
                console.log('СТАВКА СБРОШЕНА В 0');
            }
        }
    }

        stakeDisplay.addEventListener('input', function() {
            stakeDisplay._userModified = true;
            const rawValue = this.value.replace(/\s/g, '');
            stakeInput.value = rawValue;
        });

        function recalcWithFlag() {
            stakeDisplay._userModified = false;
            calculateStake();
        }

        bankInput.addEventListener('input', recalcWithFlag);
        oddsInput.addEventListener('input', recalcWithFlag);
        probInput.addEventListener('input', recalcWithFlag);
        kellySelect.addEventListener('change', recalcWithFlag);

        stakeDisplay._userModified = false;
        calculateStake();
    });