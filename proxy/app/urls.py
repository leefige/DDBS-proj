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
    # ex: /app/update/user/u1/{...}
    path('update/<str:collection>/<str:id>/<str:updatement>/', views.update, name='update'),
    # ex: /app/top/weekly/
    path('top/<str:temporal>/', views.top, name='top'),
    # ex: /app/detail/a1/image/image_a1_2.jpg
    path('detail/<str:id>/<str:file_type>/<str:file_name>', views.detail, name='detail'),
]
