document.addEventListener('DOMContentLoaded', function() {
    const checkboxes = document.querySelectorAll('.bet-select');
    const applyBtn = document.getElementById('applyBulkAction');
    const actionSelect = document.getElementById('bulkAction');
    const form = document.getElementById('bulkActionForm');
    const modal = document.getElementById('confirmModal');
    const modalBody = document.getElementById('modalBody');
    const confirmBtn = document.getElementById('confirmAction');

    // Функция обновления состояния кнопки
    function updateButtonState() {
        const checkedCount = document.querySelectorAll('.bet-select:checked').length;
        applyBtn.disabled = checkedCount === 0;
    }

    // Добавляем обработчики на чекбоксы
    checkboxes.forEach(cb => {
        cb.addEventListener('change', updateButtonState);
    });

    // Кнопка "Применить"
    applyBtn.addEventListener('click', function() {
        const action = actionSelect.value;
        if (!action) {
            alert('Выберите действие');
            return;
        }

        // Получаем выбранные чекбоксы
        const selected = document.querySelectorAll('.bet-select:checked');

        if (selected.length === 0) {
            alert('Выберите хотя бы одну ставку');
            return;
        }

        // Очищаем форму от старых данных
        while (form.children.length > 3) {
            form.removeChild(form.lastChild);
        }

        // Устанавливаем action
        document.getElementById('bulkActionInput').value = action;

        // Добавляем только выбранные ID
        selected.forEach(cb => {
            const input = document.createElement('input');
            input.type = 'hidden';
            input.name = 'selected_bets';
            input.value = cb.value;
            form.appendChild(input);
        });

        // Показываем модальное окно
        if (action === 'delete') {
            modalBody.innerHTML = `
                <p>Вы уверены, что хотите удалить <strong>${selected.length}</strong> ставок?</p>
                <p class="text-danger">Это действие нельзя отменить!</p>
            `;
        } else {
            const actionText = actionSelect.options[actionSelect.selectedIndex].text;
            modalBody.innerHTML = `
                <p>Вы уверены, что хотите ${actionText.toLowerCase()} для <strong>${selected.length}</strong> ставок?</p>
            `;
        }

        modal.classList.add('show');
    });

    // Подтверждение действия
    confirmBtn.onclick = function() {
        document.getElementById('confirmInput').value = 'true';
        form.submit();
    };

    // Функции для фильтров
    window.toggleFilters = function() {
        const form = document.getElementById('filterForm');
        const toggle = document.getElementById('filterToggle');
        if (form.style.display === 'none') {
            form.style.display = 'block';
            toggle.textContent = '▼';
        } else {
            form.style.display = 'none';
            toggle.textContent = '▶';
        }
    };

    window.closeModal = function() {
        modal.classList.remove('show');
    };

    // Закрытие по клику вне модального окна
    window.addEventListener('click', function(event) {
        if (event.target === modal) {
            closeModal();
        }
    });
});