document.addEventListener('DOMContentLoaded', function() {
    // Автодополнение для полей ввода игроков
    const inputs = document.querySelectorAll('.autocomplete-input');

    inputs.forEach(input => {
        const targetId = input.getAttribute('data-target');
        const suggestionsDiv = document.getElementById(targetId);

        input.addEventListener('input', function() {
            const query = this.value.trim();

            if (query.length < 1) {
                suggestionsDiv.style.display = 'none';
                return;
            }

            // Запрос к API
            fetch(`/bets/bet/tennis/autocomplete/?q=${encodeURIComponent(query)}`)
                .then(response => response.json())
                .then(data => {
                    if (data.length > 0) {
                        suggestionsDiv.innerHTML = '';
                        data.forEach(player => {
                            const item = document.createElement('div');
                            item.classList.add('autocomplete-item');
                            item.textContent = player;
                            item.addEventListener('click', function() {
                                input.value = player;
                                suggestionsDiv.style.display = 'none';
                            });
                            suggestionsDiv.appendChild(item);
                        });
                        suggestionsDiv.style.display = 'block';
                    } else {
                        suggestionsDiv.style.display = 'none';
                    }
                });
        });

        // Закрыть список при клике вне
        document.addEventListener('click', function(e) {
            if (!input.contains(e.target) && !suggestionsDiv.contains(e.target)) {
                suggestionsDiv.style.display = 'none';
            }
        });
    });
});