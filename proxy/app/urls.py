from django.contrib import admin
from django.urls import include, path
from . import views

urlpatterns = [
    # ex: /app/
    path('', views.index, name='index'),
    # ex: /app/user_read/u1/
    path('user_read/<str:id>/', views.user_read, name='user_read'),
    # ex: /app/query/user/u1/
    path('query/<str:collection>/<str:id>/', views.query, name='query'),
    # ex: /app/top/weekly/
    path('top/<str:temporal>/', views.top, name='top'),
]
