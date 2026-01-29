from django.shortcuts import render
from django.views.generic import TemplateView


# Create your views here.
class BetsTemplateView(TemplateView):
    template_name = 'app_bets/bets_main.html'