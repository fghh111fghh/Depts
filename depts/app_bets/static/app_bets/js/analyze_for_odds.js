document.addEventListener('DOMContentLoaded', function() {
    // Валидация диапазона коэффициентов
    const oddsFrom = document.getElementById('odds_from');
    const oddsTo = document.getElementById('odds_to');

    if (oddsFrom && oddsTo) {
        function validateRange() {
            const from = parseFloat(oddsFrom.value);
            const to = parseFloat(oddsTo.value);

            if (from > to) {
                oddsTo.setCustomValidity('Коэф. "до" должно быть больше или равно "от"');
            } else {
                oddsTo.setCustomValidity('');
            }
        }

        oddsFrom.addEventListener('input', validateRange);
        oddsTo.addEventListener('input', validateRange);
        validateRange(); // Проверка при загрузке
    }
});