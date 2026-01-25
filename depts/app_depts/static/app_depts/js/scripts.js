document.addEventListener('DOMContentLoaded', function() {

    // --- 1. АВТОМАТИЧЕСКОЕ СКРЫТИЕ УВЕДОМЛЕНИЙ (ALERTS) ---
    const messages = document.querySelectorAll('.alert');
    if (messages.length > 0) {
        setTimeout(function() {
            messages.forEach(msg => {
                msg.style.transition = "opacity 0.5s ease";
                msg.style.opacity = "0";
                setTimeout(() => msg.remove(), 500);
            });
        }, 5000); // Уведомление исчезнет через 5 секунд
    }

    // --- 2. УПРАВЛЕНИЕ МОДАЛЬНЫМИ ОКНАМИ (ОТКРЫТИЕ/ЗАКРЫТИЕ) ---
    // Функция для закрытия модалки при клике на серый фон
    const modalOverlays = document.querySelectorAll('.modal-overlay');
    modalOverlays.forEach(overlay => {
        overlay.addEventListener('click', function(e) {
            if (e.target === this) {
                this.style.display = 'none';
            }
        });
    });

    // --- 3. ЛОГИКА КНОПКИ "ОПЛАТИТЬ ВСЁ" ---
    // Ищем все кнопки с атрибутом data-full-amount
    const fullAmountButtons = document.querySelectorAll('.btn-pay-all');
    fullAmountButtons.forEach(button => {
        button.addEventListener('click', function() {
            const targetId = this.getAttribute('data-target-input');
            const balance = this.getAttribute('data-balance');
            const input = document.getElementById(targetId);
            if (input) {
                // Заменяем запятую на точку для корректной работы input type="number"
                input.value = balance.replace(',', '.');
            }
        });
    });

    // --- 4. МАСКА ДЛЯ ТЕЛЕФОНА (ДЛЯ ФОРМ В АДМИНКЕ ИЛИ КРЕДИТОРАХ) ---
    document.addEventListener('input', function (e) {
        if (e.target.name && e.target.name.includes('phone')) {
            let input = e.target;
            let value = input.value.replace(/\D/g, ''); // Удаляем всё кроме цифр

            if (value.length > 0 && (value[0] === '7' || value[0] === '8')) {
                value = value.substring(1);
            }

            let formattedValue = '+7 ';
            if (value.length > 0) formattedValue += '(' + value.substring(0, 3);
            if (value.length >= 3) formattedValue += ') ' + value.substring(3, 6);
            if (value.length >= 6) formattedValue += '-' + value.substring(6, 8);
            if (value.length >= 8) formattedValue += '-' + value.substring(8, 10);

            input.value = formattedValue;
        }
    });

});

// Глобальные функции для кнопок (если они прописаны в onclick в HTML)
function openModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) modal.style.display = 'flex';
}

function closeModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) modal.style.display = 'none';
}