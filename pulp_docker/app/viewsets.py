"""
Check `Plugin Writer's Guide`_ for more details.

.. _Plugin Writer's Guide:
    http://docs.pulpproject.org/en/3.0/nightly/plugins/plugin-writer/index.html
"""

from django_filters import CharFilter, MultipleChoiceFilter
from django.db import transaction
from drf_yasg.utils import swagger_auto_schema

from pulpcore.plugin.serializers import (
    AsyncOperationResponseSerializer,
    RepositorySyncURLSerializer,
)

from pulpcore.plugin.tasking import enqueue_with_reservation
from pulpcore.plugin.viewsets import (
    BaseDistributionViewSet,
    ContentFilter,
    ContentViewSet,
    RemoteViewSet,
    OperationPostponedResponse,)
from rest_framework.decorators import action

from . import models, serializers, tasks


class ManifestTagFilter(ContentFilter):
    """
    FilterSet for Tags.
    """

    media_type = MultipleChoiceFilter(
        choices=models.Manifest.MANIFEST_CHOICES,
        field_name='tagged_manifest__media_type',
        lookup_expr='contains',
    )
    digest = CharFilter(field_name='tagged_manifest__digest')

    class Meta:
        model = models.ManifestTag
        fields = {
            'name': ['exact', 'in'],
        }


class ManifestFilter(ContentFilter):
    """
    FilterSet for Manifests.
    """

    media_type = MultipleChoiceFilter(choices=models.Manifest.MANIFEST_CHOICES)

    class Meta:
        model = models.Manifest
        fields = {
            'digest': ['exact', 'in'],
        }


class ManifestTagViewSet(ContentViewSet):
    """
    ViewSet for ManifestTag.
    """

    endpoint_name = 'manifest-tags'
    queryset = models.ManifestTag.objects.all()
    serializer_class = serializers.ManifestTagSerializer
    filterset_class = ManifestTagFilter

    @transaction.atomic
    def create(self, request):
        """
        Create a new ManifestTag from a request.
        """
        raise NotImplementedError()


class ManifestViewSet(ContentViewSet):
    """
    ViewSet for Manifest.
    """

    endpoint_name = 'manifests'
    queryset = models.Manifest.objects.all()
    serializer_class = serializers.ManifestSerializer
    filterset_class = ManifestFilter

    @transaction.atomic
    def create(self, request):
        """
        Create a new Manifest from a request.
        """
        raise NotImplementedError()


class BlobFilter(ContentFilter):
    """
    FilterSet for Blobs.
    """

    media_type = MultipleChoiceFilter(choices=models.ManifestBlob.BLOB_CHOICES)

    class Meta:
        model = models.ManifestBlob
        fields = {
            'digest': ['exact', 'in'],
        }


class BlobViewSet(ContentViewSet):
    """
    ViewSet for ManifestBlobs.
    """

    endpoint_name = 'blobs'
    queryset = models.ManifestBlob.objects.all()
    serializer_class = serializers.BlobSerializer
    filterset_class = BlobFilter

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
    @action(detail=True, methods=['post'], serializer_class=RepositorySyncURLSerializer)
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


class DockerDistributionViewSet(BaseDistributionViewSet):
    """
    ViewSet for DockerDistribution model.
    """

    endpoint_name = 'docker'
    queryset = models.DockerDistribution.objects.all()
    serializer_class = serializers.DockerDistributionSerializer
