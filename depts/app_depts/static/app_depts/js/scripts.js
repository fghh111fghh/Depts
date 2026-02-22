document.addEventListener('DOMContentLoaded', function() {
    // --- ПОИСК ТОЛЬКО ПО ENTER ---
    const searchInput = document.getElementById('search-input');
    const searchForm = document.querySelector('.search-form');

    if (searchInput) {
        console.log('Поиск найден!');

        // Поиск только по Enter
        searchInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                e.preventDefault();
                console.log('Enter: ' + searchInput.value);
                searchForm.submit();
            }
        });
    }

    // --- Скрытие сообщений ---
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
});

// Глобальные функции для модалок
function openModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) modal.style.display = 'flex';
}

function closeModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) modal.style.display = 'none';
}