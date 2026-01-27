document.addEventListener('DOMContentLoaded', function() {

    // --- 1. АВТОМАТИЧЕСКОЕ СКРЫТИЕ УВЕДОМЛЕНИЙ ---
    const messages = document.querySelectorAll('.alert');
    if (messages.length > 0) {
        setTimeout(function() {
            messages.forEach(msg => {
                msg.style.transition = "opacity 0.5s ease";
                msg.style.opacity = "0";
                setTimeout(() => msg.remove(), 500);
            });
        }, 5000);
    }

    // --- 3. МАСКА ДЛЯ ТЕЛЕФОНА (+7) ---
    document.addEventListener('input', function (e) {
        if (e.target.name && e.target.name.includes('phone')) {
            let input = e.target;
            let value = input.value.replace(/\D/g, '');

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

    // Блокировка удаления префикса +7
    document.addEventListener('keydown', function (e) {
        if (e.target.name && e.target.name.includes('phone')) {
            if (e.target.selectionStart <= 3 && (e.keyCode === 8 || e.keyCode === 46)) {
                e.preventDefault();
            }
        }
    });
});

// Глобальные функции для модалок (используются в кнопках карточек)
function openModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) modal.style.display = 'flex';
}

function closeModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) modal.style.display = 'none';
}