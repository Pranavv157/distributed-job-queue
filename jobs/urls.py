from django.urls import path
from .views import CreateJobAPI

urlpatterns = [
    path("create/", CreateJobAPI.as_view()),
]