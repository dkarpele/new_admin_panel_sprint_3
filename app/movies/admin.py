from django.contrib import admin
from django.utils.translation import gettext_lazy as _

from .models import Genre, Filmwork, GenreFilmwork, Person, PersonFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    # Отображение полей в списке
    list_display = (
        'name', 'description', 'created', 'modified')

    # Поиск по полям
    search_fields = ('name', 'description', 'id')


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork
    autocomplete_fields = ('person',)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    # Отображение полей в списке
    list_display = ('full_name', 'modified')

    # Поиск по полям
    search_fields = ('full_name', 'id')


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork
    autocomplete_fields = ('genre',)


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline,)

    # Отображение полей в списке
    list_display = (
        'title', 'type', 'creation_date', 'rating', 'created', 'modified',
        'get_genres')

    list_prefetch_related = 'genres'

    def get_queryset(self, request):
        queryset = super().get_queryset(request)
        return queryset.prefetch_related(self.list_prefetch_related)

    def get_genres(self, obj):
        return ', '.join([genre.name for genre in obj.genres.all()])
    get_genres.short_description = _('film_genres')

    # Фильтрация в списке
    list_filter = ('type', 'rating')

    # Поиск по полям
    search_fields = ('title', 'description', 'id')
