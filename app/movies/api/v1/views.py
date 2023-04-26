from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q, QuerySet
from django.http import JsonResponse
from django.views.generic.list import BaseListView
from django.views.generic.detail import BaseDetailView
from django.shortcuts import render

from movies.models import Filmwork, PersonFilmwork


class MoviesApiMixin:
    model = Filmwork
    http_method_names = ['get']

    def query_set(self):
        values_tuple = 'id', 'title', 'description', 'creation_date', \
            'rating', 'type'

        def filter_person(role):
            return ArrayAgg(
                'persons__full_name',
                filter=Q(personfilmwork__role=role),
                distinct=True)

        query_set = self.model.objects. \
            prefetch_related('genres', 'persons'). \
            values(*values_tuple). \
            annotate(genres=ArrayAgg(
                         'genres__name',
                         distinct=True),
                     actors=filter_person(PersonFilmwork.Role.ACTOR),
                     writers=filter_person(PersonFilmwork.Role.WRITER),
                     directors=filter_person(PersonFilmwork.Role.DIRECTOR),
                     ).order_by('id')
        return query_set

    def get_queryset(self) -> QuerySet:
        try:
            if self.kwargs['pk']:
                return self.query_set().filter(id=f'{str(self.kwargs["pk"])}')
        except KeyError:
            return self.query_set()

    def render_to_response(self, context, **response_kwargs):
        print()
        return JsonResponse(context, json_dumps_params={'indent': 2})


class MoviesListApi(MoviesApiMixin, BaseListView):
    def get_context_data(self, *, object_list=None, **kwargs):
        query_set = self.get_queryset()
        page_size = 50
        paginator, page, query_set, is_paginated = self.paginate_queryset(
             query_set, page_size)

        prev_page = None if page.number == 1 else page.number - 1
        next_page = None if page.number == paginator.num_pages \
            else page.number + 1

        context = {
            "count": paginator.count,
            "total_pages": paginator.num_pages,
            "prev": prev_page,
            "next": next_page,
            "results": list(query_set),
        }
        return context


class MoviesDetailApi(MoviesApiMixin, BaseDetailView):
    def get_context_data(self, **kwargs):
        queryset = self.get_queryset()
        context = list(queryset)[0]
        return context


def page_not_found_view(request, exception):
    return render(request, '404.html', status=404)
