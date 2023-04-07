import uuid

from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _


# Create your models here.
class TimeStampedMixin(models.Model):
    created = models.DateTimeField(_('created'), auto_now_add=True)
    modified = models.DateTimeField(_('modified'), auto_now=True)

    class Meta:
        # Этот параметр указывает Django, что этот класс не является
        # представлением таблицы
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    # Первым аргументом обычно идёт человекочитаемое название поля
    name = models.CharField(_('name'), max_length=255, unique=True)
    # blank=True делает поле необязательным для заполнения.
    description = models.TextField(_('description'), blank=True)
    # auto_now_add автоматически выставит дату создания записи

    def __str__(self):
        return self.name

    class Meta:
        # Ваши таблицы находятся в нестандартной схеме.
        # Это нужно указать в классе модели
        db_table = "content\".\"genre"
        # Следующие два поля отвечают за название модели в интерфейсе
        verbose_name = _('verbose_name_genre')
        verbose_name_plural = _('verbose_name_genre_plural')


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('full_name'), max_length=255)

    def __str__(self):
        return self.full_name

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('verbose_name_person')
        verbose_name_plural = _('verbose_name_person_plural')


class Filmwork(UUIDMixin, TimeStampedMixin):
    class Type(models.TextChoices):
        MOVIE = 'movie', (_('movie'))
        TV_SHOW = 'tv_show', (_('tv_show'))

    title = models.CharField(_('title'), max_length=255)
    description = models.TextField(_('description'), blank=True)
    creation_date = models.DateField(_('creation_date'), blank=True)
    rating = models.FloatField(_('rating'),
                               blank=True,
                               validators=[MinValueValidator(0),
                                           MaxValueValidator(100)])
    type = models.CharField(_('type'),
                            max_length=50,
                            choices=Type.choices,
                            default=Type.MOVIE,
                            )
    genres = models.ManyToManyField(Genre, through='GenreFilmwork',)
    persons = models.ManyToManyField(Person, through='PersonFilmwork',)

    def __str__(self):
        return self.title

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('verbose_name_filmwork')
        verbose_name_plural = _('verbose_name_filmwork_plural')
        ordering = ['-id']


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork,
                                  on_delete=models.CASCADE,
                                  verbose_name=_('film_work'),
                                  related_name='genrefilmwork',)
    genre = models.ForeignKey(Genre,
                              on_delete=models.CASCADE,
                              verbose_name=_('genre'),
                              related_name='genrefilmwork')
    created = models.DateTimeField(_('created'), auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        verbose_name = _('genrefilmwork')
        verbose_name_plural = _('genrefilmwork_plural')
        constraints = [
            models.UniqueConstraint(fields=['film_work', 'genre'],
                                    name='film_work_genre')
        ]


class PersonFilmwork(UUIDMixin):
    class Role(models.TextChoices):
        DIRECTOR = 'director', (_('director'))
        WRITER = 'writer', (_('writer'))
        ACTOR = 'actor', (_('actor'))
    film_work = models.ForeignKey(Filmwork,
                                  on_delete=models.CASCADE,
                                  verbose_name=_('film_work'),
                                  related_name='personfilmwork')
    person = models.ForeignKey(Person,
                               on_delete=models.CASCADE,
                               verbose_name=_('person'),
                               related_name='personfilmwork')
    role = models.CharField(_('role'),
                            max_length=50,
                            choices=Role.choices,
                            default=Role.DIRECTOR)
    created = models.DateTimeField(_('created'), auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        verbose_name = _('personfilmwork')
        verbose_name_plural = _('personfilmwork_plural')
        constraints = [
            models.UniqueConstraint(fields=['film_work', 'person', 'role'],
                                    name='film_work_person_role')
        ]
