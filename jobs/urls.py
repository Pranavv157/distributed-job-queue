from django.urls import path
from .views import JobAPI

urlpatterns = [
    path("create/", JobAPI.as_view()),
    path("<uuid:job_id>/", JobAPI.as_view()),
]