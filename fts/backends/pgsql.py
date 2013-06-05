"Pgsql Fts backend"

from django.db import connection, transaction, connections
from django.db.models.fields import FieldDoesNotExist
from django.db.models.query import QuerySet
from django.db.models.sql.query import Query
from django.db.models.sql.compiler import SQLCompiler

from fts.backends.base import InvalidFtsBackendError
from fts.backends.base import BaseClass, BaseModel, BaseManager

qn = connection.ops.quote_name

from django.db import models
LANGUAGES = {
    '' : 'simple',
    'da' : 'danish',
    'nl' : 'dutch',
    'en' : 'english',
    'fi' : 'finnish',
    'fr' : 'french',
    'de' : 'german',
    'hu' : 'hungarian',
    'it' : 'italian',
    'no' : 'norwegian',
    'pt' : 'portuguese',
    'ro' : 'romanian',
    'ru' : 'russian',
    'es' : 'spanish',
    'sv' : 'swedish',
    'tr' : 'turkish',
}

class VectorField(models.Field):
    def __init__(self, *args, **kwargs):
        kwargs['null'] = True
        kwargs['editable'] = False
        kwargs['serialize'] = False
        super(VectorField, self).__init__(*args, **kwargs)
    
    def db_type(self, connection=None):
        return 'tsvector'

    def contribute_to_class(self, cls, name):
        name = getattr(cls, 'search_vector_field', name)
        self.db_column = name
        super(VectorField, self).contribute_to_class(cls, name)

class SearchClass(BaseClass):
    def __init__(self, server, params):
        self.backend = 'pgsql'

class SearchManager(BaseManager):
    def __init__(self, **kwargs):
        super(SearchManager, self).__init__(**kwargs)
        self.language = LANGUAGES[self.language_code]
        self._vector_field_cache = None

    def _get_query_set(self):
        qs = SearchQuerySet(self.model)
        qs.vector_field = self.vector_field
        qs.language = self.language
        return qs

    def _vector_field(self):
        """
        Returns the VectorField defined for this manager's model. There must be exactly one VectorField defined.
        """
        if self._vector_field_cache is not None:
            return self._vector_field_cache
        
        vectors = [f for f in self.model._meta.fields if isinstance(f, VectorField)]
        
        if len(vectors) != 1:
            raise ValueError('There must be exactly 1 VectorField defined for the %s model.' % self.model._meta.object_name)
            
        self._vector_field_cache = vectors[0]
        
        return self._vector_field_cache
    vector_field = property(_vector_field)
    
    def _vector_sql(self, field, weight):
        """
        Returns the SQL used to build a tsvector from the given (django) field name.
        """
        try:
            f = self.model._meta.get_field(field)
            return ("setweight(to_tsvector('%s', coalesce(%s,'')), '%s')" % (self.language, qn(f.column), weight), [])
        except FieldDoesNotExist:
            return ("setweight(to_tsvector('%s', %%s), '%s')" % (self.language, weight), [field])

    def _update_index_update(self, pk=None):
        # Build a list of SQL clauses that generate tsvectors for each specified field.
        clauses = []
        params = []
        for field, weight in self._fields.items():
            v = self._vector_sql(field, weight)
            clauses.append(v[0])
            params.extend(v[1])
        vector_sql = ' || '.join(clauses)
        
        where = ''
        # If one or more pks are specified, tack a WHERE clause onto the SQL.
        if pk is not None:
            if isinstance(pk, (list,tuple)):
                ids = ','.join(str(v) for v in pk)
                where = ' WHERE %s IN (%s)' % (qn(self.model._meta.pk.column), ids)
            else:
                where = ' WHERE %s = %d' % (qn(self.model._meta.pk.column), pk)
        sql = 'UPDATE %s SET %s = %s%s' % (qn(self.model._meta.db_table), qn(self.vector_field.column), vector_sql, where)
        cursor = connection.cursor()
        cursor.execute(sql, tuple(params))
        transaction.set_dirty()

    def _update_index_walking(self, pk=None):
        if pk is not None:
            if isinstance(pk, (list,tuple)):
                items = self.filter(pk__in=pk)
            else:
                items = self.filter(pk=pk)
        else:
            items = self.all()
        
        IW = {}
        for item in items:
            clauses = []
            params = []
            for field, weight in self._fields.items():
                if callable(field):
                    words = field(item)
                elif '__' in field:
                    words = item
                    for col in field.split('__'):
                        words = getattr(words, col)
                else:
                    words = field
                v = self._vector_sql(words, weight)
                clauses.append(v[0])
                params.extend(v[1])
            vector_sql = ' || '.join(clauses)
            sql = 'UPDATE %s SET %s = %s WHERE %s = %d' % (qn(self.model._meta.db_table), qn(self.vector_field.column), vector_sql, qn(self.model._meta.pk.column), item.pk)
            cursor = connection.cursor()
            cursor.execute(sql, tuple(params))
        transaction.set_dirty()
    
    @transaction.commit_on_success
    def _update_index(self, pk=None):
        index_walking = False
        for field, weight in self._fields.items():
            if callable(field) or '__' in field:
                index_walking = True
                break
        if index_walking:
            self._update_index_walking(pk)
        else:
            self._update_index_update(pk)


class SearchQuerySet(QuerySet):

    class SearchQuery(Query):

        class SearchCompiler(SQLCompiler):
            def get_from_clause(self):
                from_, f_params = super(SearchQuerySet.SearchQuery.SearchCompiler, self).get_from_clause()
                if hasattr(self.query, "ts_query"):
                    ts_query = ", %s('%s', %%s) as ts_query" % (
                        self.query.ts_function,
                        self.query.ts_language)
                    from_.append(ts_query)
                    f_params.append(self.query.ts_query)
                return from_, f_params

        def get_compiler(self, using=None, connection=None):
            """ Overrides the Query method get_compiler in order to return
                an instance of the above custom compiler.
            """
            # Copy the body of this method from Django except the final
            # return statement. We will ignore code coverage for this.
            if using is None and connection is None: #pragma: no cover
                raise ValueError("Need either using or connection")
            if using:
                connection = connections[using]
                # Check that the compiler will be able to execute the query
            for alias, aggregate in self.aggregate_select.items():
                connection.ops.check_aggregate_support(aggregate)
                # Instantiate the custom compiler.
            return self.SearchCompiler(self, connection, using)

        def clone(self, klass=None, memo=None, **kwargs):
            _clone = super(SearchQuerySet.SearchQuery, self).clone(klass, memo, **kwargs)
            if hasattr(self, "ts_language"):
                _clone.ts_language = self.ts_language
            if hasattr(self, "ts_function"):
                _clone.ts_function = self.ts_function
            if hasattr(self, "ts_query"):
                _clone.ts_query = self.ts_query
            return _clone

    def __init__(self, model=None, query=None, using=None):
        if query is None:
            query = self.SearchQuery(model)
        super(SearchQuerySet, self).__init__(model=model, query=query, using=using)

    class SearchWhere(object):
        def __init__(self, alias, column, query):
            self.alias = alias
            self.column = column
            self.query = query

        def as_sql(self, qn=None, connection=None):
            sql = '%s.%s @@ %s' % (qn(self.alias), qn(self.column), self.query)
            return (sql, [])

        def relabel_aliases(self, change_map):
            if self.alias in change_map:
                self.alias = change_map[self.alias]


    def search(self, query, query_type='plain', **kwargs):
        """
        Returns a queryset after having applied the full-text search query. If rank_field
        is specified, it is the name of the field that will be put on each returned instance.
        When specifying a rank_field, the results will automatically be ordered by -rank_field.

        query_type='' specifies the use of to_tsquery. The query_type is prefixed to ts_query.
        Also None can be used.

        For possible rank_normalization values, refer to:
        http://www.postgresql.org/docs/8.3/static/textsearch-controls.html#TEXTSEARCH-RANKING
        """
        rank_field = kwargs.get("rank_field", "search_rank")
        rank_normalization = kwargs.get("rank_normalization", 32)

        func_name = "%sto_tsquery" % (query_type if query_type else '')
        ts_query = "ts_query"
        where = '%s.%s @@ %s' % (qn(self.model._meta.db_table), qn(self.vector_field.column), ts_query)

        select = {}
        order = []
        if rank_field is not None:
            select[rank_field] = 'ts_rank_cd(%s.%s, %s, %d)' % (qn(self.model._meta.db_table), qn(self.vector_field.column), ts_query, rank_normalization)
            order = ['-%s' % rank_field]

        # return self.extra(select=select, where=[where], order_by=order)
        #
        clone = self.extra(select=select, order_by=order)

        where = SearchQuerySet.SearchWhere(self.model._meta.db_table,
                                           self.vector_field.column,
                                           ts_query)
        clone.query.where.add(where, "AND")
        clone.query.ts_language = self.language
        clone.query.ts_function = func_name
        clone.query.ts_query = query
        return clone


    def _clone(self, *args, **kwargs):
        """
        Ensure attributes are copied to subsequent queries.
        """
        for attr in ("vector_field", "language"):
            kwargs[attr] = getattr(self, attr)
        return super(SearchQuerySet, self)._clone(*args, **kwargs)

class SearchableModel(BaseModel):
    class Meta:
        abstract = True

    search_index = VectorField()

    objects = SearchManager()

try:
    from south.modelsinspector import add_introspection_rules
    add_introspection_rules([], ["^fts\.backends\.pgsql\.VectorField"])
except ImportError:
    pass

