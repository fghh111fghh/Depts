// main.js
document.addEventListener('DOMContentLoaded', function() {
    console.log("Система учета долгов готова.");

    // Пример: подсветка карточки при клике
    const cards = document.querySelectorAll('.record-card');
    cards.forEach(card => {
        card.addEventListener('click', () => {
            cards.forEach(c => c.style.outline = "none");
            card.style.outline = "2px solid #3498db";
        });
    });
});