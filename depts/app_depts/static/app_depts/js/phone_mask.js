document.addEventListener('input', function (e) {
    // 1. Проверяем, что ввод идет в поле, имя которого содержит 'phone'
    if (e.target.name && e.target.name.includes('phone')) {
        let input = e.target;
        let value = input.value.replace(/\D/g, ''); // Удаляем всё, кроме цифр
        let formattedValue = '';

        // 2. Если пользователь начал вводить с 7 или 8, игнорируем эту цифру,
        // так как +7 мы добавим статично
        if (value.length > 0) {
            if (value[0] === '7' || value[0] === '8') {
                value = value.substring(1);
            }
        }

        // 3. Формируем маску по мере ввода цифр
        formattedValue = '+7 ';

        if (value.length > 0) {
            formattedValue += '(' + value.substring(0, 3);
        }
        if (value.length >= 3) {
            formattedValue += ') ' + value.substring(3, 6);
        }
        if (value.length >= 6) {
            formattedValue += '-' + value.substring(6, 8);
        }
        if (value.length >= 8) {
            formattedValue += '-' + value.substring(8, 10);
        }

        // 4. Ограничиваем ввод (не более 11 цифр всего, включая скрытую 7)
        if (value.length > 10) {
            formattedValue = input.value.substring(0, 18);
        }

        // 5. Выводим результат в поле
        input.value = formattedValue;
    }
});

// Дополнительно: блокируем удаление префикса "+7 "
document.addEventListener('keydown', function (e) {
    if (e.target.name && e.target.name.includes('phone')) {
        if (e.target.selectionStart <= 3 && (e.keyCode === 8 || e.keyCode === 46)) {
            e.preventDefault();
        }
    }
});