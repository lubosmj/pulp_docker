"""
Check `Plugin Writer's Guide`_ for more details.

.. _Plugin Writer's Guide:
    http://docs.pulpproject.org/en/3.0/nightly/plugins/plugin-writer/index.html
"""

from django.db import transaction
from drf_yasg.utils import swagger_auto_schema

from pulpcore.plugin.serializers import (
    AsyncOperationResponseSerializer,
    RepositoryPublishURLSerializer,
    RepositorySyncURLSerializer,
)
from pulpcore.plugin.tasking import enqueue_with_reservation
from pulpcore.plugin.viewsets import (
    ContentViewSet,
    NamedModelViewSet,
    RemoteViewSet,
    OperationPostponedResponse,
    PublisherViewSet)
from rest_framework.decorators import detail_route
from rest_framework import mixins

from . import models, serializers, tasks


class ManifestListTagViewSet(ContentViewSet):
    """
    ViewSet for ManifestListTag.
    """

    endpoint_name = 'manifest-list-tags'
    queryset = models.ManifestListTag.objects.all()
    serializer_class = serializers.ManifestListTagSerializer

    @transaction.atomic
    def create(self, request):
        """
        Create a new ManifestListTag from a request.
        """
        raise NotImplementedError()


class ManifestTagViewSet(ContentViewSet):
    """
    ViewSet for ManifestTag.
    """

    endpoint_name = 'manifest-tags'
    queryset = models.ManifestTag.objects.all()
    serializer_class = serializers.ManifestTagSerializer

    @transaction.atomic
    def create(self, request):
        """
        Create a new ManifestTag from a request.
        """
        raise NotImplementedError()


class ManifestListViewSet(ContentViewSet):
    """
    ViewSet for ManifestList.
    """

    endpoint_name = 'manifest-lists'
    queryset = models.ManifestList.objects.all()
    serializer_class = serializers.ManifestListSerializer

    @transaction.atomic
    def create(self, request):
        """
        Create a new ManifestList from a request.
        """
        raise NotImplementedError()


class ManifestViewSet(ContentViewSet):
    """
    ViewSet for Manifest.
    """

    endpoint_name = 'manifests'
    queryset = models.ImageManifest.objects.all()
    serializer_class = serializers.ManifestSerializer

    @transaction.atomic
    def create(self, request):
        """
        Create a new Manifest from a request.
        """
        raise NotImplementedError()


class BlobViewSet(ContentViewSet):
    """
    ViewSet for ManifestBlobs.
    """

    endpoint_name = 'blobs'
    queryset = models.ManifestBlob.objects.all()
    serializer_class = serializers.BlobSerializer

    @transaction.atomic
    def create(self, request):
        """
        Create a new ManifestBlob from a request.
        """
        raise NotImplementedError()


class DockerRemoteViewSet(RemoteViewSet):
    """
    A ViewSet for DockerRemote.
    """

    endpoint_name = 'docker'
    queryset = models.DockerRemote.objects.all()
    serializer_class = serializers.DockerRemoteSerializer

    # This decorator is necessary since a sync operation is asyncrounous and returns
    # the id and href of the sync task.
    @swagger_auto_schema(
        operation_description="Trigger an asynchronous task to sync content",
        responses={202: AsyncOperationResponseSerializer}
    )
    @detail_route(methods=('post',), serializer_class=RepositorySyncURLSerializer)
    def sync(self, request, pk):
        """
        Synchronizes a repository. The ``repository`` field has to be provided.
        """
        remote = self.get_object()
        serializer = RepositorySyncURLSerializer(data=request.data, context={'request': request})

        # Validate synchronously to return 400 errors.
        serializer.is_valid(raise_exception=True)
        repository = serializer.validated_data.get('repository')
        result = enqueue_with_reservation(
            tasks.synchronize,
            [repository, remote],
            kwargs={
                'remote_pk': remote.pk,
                'repository_pk': repository.pk
            }
        )
        return OperationPostponedResponse(result, request)


class DockerPublisherViewSet(PublisherViewSet):
    """
    A ViewSet for DockerPublisher.
    """

    endpoint_name = 'docker'
    queryset = models.DockerPublisher.objects.all()
    serializer_class = serializers.DockerPublisherSerializer

    # This decorator is necessary since a publish operation is asyncrounous and returns
    # the id and href of the publish task.
    @swagger_auto_schema(
        operation_description="Trigger an asynchronous task to publish content",
        responses={202: AsyncOperationResponseSerializer}
    )
    @detail_route(methods=('post',), serializer_class=RepositoryPublishURLSerializer)
    def publish(self, request, pk):
        """
        Publishes a repository.

        Either the ``repository`` or the ``repository_version`` fields can
        be provided but not both at the same time.
        """
        publisher = self.get_object()
        serializer = RepositoryPublishURLSerializer(
            data=request.data,
            context={'request': request}
        )
        serializer.is_valid(raise_exception=True)
        repository_version = serializer.validated_data.get('repository_version')

        result = enqueue_with_reservation(
            tasks.publish,
            [repository_version.repository, publisher],
            kwargs={
                'publisher_pk': str(publisher.pk),
                'repository_version_pk': str(repository_version.pk)
            }
        )
        return OperationPostponedResponse(result, request)


class DockerDistributionViewSet(NamedModelViewSet,
                                mixins.UpdateModelMixin,
                                mixins.RetrieveModelMixin,
                                mixins.ListModelMixin,
                                mixins.DestroyModelMixin):
    """
    ViewSet for DockerDistribution model.
    """

    endpoint_name = 'docker-distributions'
    queryset = models.DockerDistribution.objects.all()
    serializer_class = serializers.DockerDistributionSerializer

    @swagger_auto_schema(operation_description="Trigger an asynchronous create task",
                         responses={202: AsyncOperationResponseSerializer})
    def create(self, request, *args, **kwargs):
        """
        Dispatches a task with reservation for creating a docker distribution.
        """
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        async_result = enqueue_with_reservation(
            tasks.distribution.create,
            "/api/v3/docker-distributions/",
            kwargs={'data': request.data}
        )
        return OperationPostponedResponse(async_result, request)

    @swagger_auto_schema(operation_description="Trigger an asynchronous update task",
                         responses={202: AsyncOperationResponseSerializer})
    def update(self, request, pk, *args, **kwargs):
        """
        Dispatches a task with reservation for updating a docker distribution.
        """
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        async_result = enqueue_with_reservation(
            tasks.distribution.update,
            "/api/v3/docker-distributions/",
            args=(pk,),
            kwargs={'data': request.data, 'partial': partial}
        )
        return OperationPostponedResponse(async_result, request)

    @swagger_auto_schema(operation_description="Trigger an asynchronous partial update task",
                         responses={202: AsyncOperationResponseSerializer})
    def partial_update(self, request, *args, **kwargs):
        """
        Dispatches a task with reservation for partially updating a docker distribution.
        """
        kwargs['partial'] = True
        return self.update(request, *args, **kwargs)

    @swagger_auto_schema(operation_description="Trigger an asynchronous delete task",
                         responses={202: AsyncOperationResponseSerializer})
    def delete(self, request, pk, *args, **kwargs):
        """
        Dispatches a task with reservation for deleting a docker distribution.
        """
        self.get_object()
        async_result = enqueue_with_reservation(
            tasks.distribution.delete,
            "/api/v3/docker-distributions/",
            args=(pk,)
        )
        return OperationPostponedResponse(async_result, request)