from gettext import gettext as _

from django.conf import settings

from rest_framework import serializers

from pulpcore.plugin.models import (
    Remote,
    Repository,
    RepositoryVersion,
    RepositoryContent
)
from pulpcore.plugin.serializers import (
    DetailRelatedField,
    RemoteSerializer,
    RepositoryVersionDistributionSerializer,
    SingleArtifactContentSerializer,
    RelatedField,
    NestedRelatedField,
)

from . import models


class ManifestTagSerializer(SingleArtifactContentSerializer):
    """
    Serializer for ManifestTags.
    """

    name = serializers.CharField(help_text="Tag name")
    tagged_manifest = DetailRelatedField(
        many=False,
        help_text="Manifest that is tagged",
        view_name='docker-manifests-detail',
        queryset=models.Manifest.objects.all()
    )

    class Meta:
        fields = SingleArtifactContentSerializer.Meta.fields + (
            'name',
            'tagged_manifest',
        )
        model = models.ManifestTag


class ManifestSerializer(SingleArtifactContentSerializer):
    """
    Serializer for Manifests.
    """

    digest = serializers.CharField(help_text="sha256 of the Manifest file")
    schema_version = serializers.IntegerField(help_text="Docker schema version")
    media_type = serializers.CharField(help_text="Docker media type of the file")
    listed_manifests = DetailRelatedField(
        many=True,
        help_text="Manifests that are referenced by this Manifest List",
        view_name='docker-manifests-detail',
        queryset=models.Manifest.objects.all()
    )
    blobs = DetailRelatedField(
        many=True,
        help_text="Blobs that are referenced by this Manifest",
        view_name='docker-blobs-detail',
        queryset=models.ManifestBlob.objects.all()
    )
    config_blob = DetailRelatedField(
        many=False,
        help_text="Blob that contains configuration for this Manifest",
        view_name='docker-blobs-detail',
        queryset=models.ManifestBlob.objects.all()
    )

    class Meta:
        fields = SingleArtifactContentSerializer.Meta.fields + (
            'digest',
            'schema_version',
            'media_type',
            'listed_manifests',
            'config_blob',
            'blobs',
        )
        model = models.Manifest


class BlobSerializer(SingleArtifactContentSerializer):
    """
    Serializer for Blobs.
    """

    digest = serializers.CharField(help_text="sha256 of the Blob file")
    media_type = serializers.CharField(help_text="Docker media type of the file")

    class Meta:
        fields = SingleArtifactContentSerializer.Meta.fields + (
            'digest',
            'media_type',
        )
        model = models.ManifestBlob


class RegistryPathField(serializers.CharField):
    """
    Serializer Field for the registry_path field of the DockerDistribution.
    """

    def to_representation(self, value):
        """
        Converts a base_path into a registry path.
        """
        if settings.CONTENT_HOST:
            host = settings.CONTENT_HOST
        else:
            host = self.context['request'].get_host()
        return ''.join([host, '/', value])


class DockerRemoteSerializer(RemoteSerializer):
    """
    A Serializer for DockerRemote.

    Add any new fields if defined on DockerRemote.
    Similar to the example above, in DockerContentSerializer.
    Additional validators can be added to the parent validators list

    For example::

    class Meta:
        validators = platform.RemoteSerializer.Meta.validators + [myValidator1, myValidator2]
    """

    upstream_name = serializers.CharField(
        required=True,
        allow_blank=False,
        help_text=_("Name of the upstream repository")
    )
    whitelist_tags = serializers.CharField(
        required=False,
        allow_null=True,
        help_text="""A comma separated string of tags to sync.
        Example:

        latest,1.27.0
        """
    )

    policy = serializers.ChoiceField(
        help_text="The policy to use when downloading content.",
        choices=Remote.POLICY_CHOICES,
        default=Remote.IMMEDIATE
    )

    class Meta:
        fields = RemoteSerializer.Meta.fields + ('upstream_name', 'whitelist_tags',)
        model = models.DockerRemote


class DockerDistributionSerializer(RepositoryVersionDistributionSerializer):
    """
    A serializer for DockerDistribution.
    """

    registry_path = RegistryPathField(
        source='base_path', read_only=True,
        help_text=_('The Registry hostame:port/name/ to use with docker pull command defined by '
                    'this distribution.')
    )

    class Meta:
        model = models.DockerDistribution
        fields = tuple(set(RepositoryVersionDistributionSerializer.Meta.fields) - {'base_url'}) + (
            'registry_path',)


class TagOperationSerializer(serializers.Serializer):
    """
    A base serializer for tagging and untagging manifests.
    """

    repository = RelatedField(
        required=False,
        view_name='repositories-detail',
        queryset=Repository.objects.all(),
        help_text='A URI of the repository.'
    )
    repository_version = NestedRelatedField(
        required=False,
        view_name='versions-detail',
        queryset=RepositoryVersion.objects.all(),
        lookup_field='number',
        parent_lookup_kwargs={'repository_pk': 'repository__pk'},
        help_text='A URI of the repository version'
    )
    tag = serializers.CharField(
        required=True,
        help_text='A tag name'
    )

    def validate(self, data):
        """
        Request's data are validated and adjusted in this method.

        A new dictionary object is initialized by the input data and altered afterwards.
        When a repository version is specified only, a particular repository object is
        retrieved from it and stored in the dictionary.
        """
        repository = data.get('repository', None)
        repository_version = data.get('repository_version', None)

        new_data = {}
        new_data.update(data)

        if repository is None:
            if repository_version is None:
                raise serializers.ValidationError(
                    _("Either 'repository' or 'repository_version' needs to be specified"))
            else:
                new_data['repository'] = repository_version.repository

        return new_data


class TagImageSerializer(TagOperationSerializer):
    """
    A serializer for parsing and validating data associated with a manifest tagging.
    """

    digest = serializers.CharField(
        required=True,
        help_text='sha256 of the Manifest file'
    )

    def validate(self, data):
        """
        Validate data passed through a request call.

        In addition to the inherited method, Manifest with a corresponding digest is
        retrieved from a database and stored in the dictionary to avoid querying the
        database in the ViewSet again. The method checks if the tag exists within the
        repository.
        """
        new_data = super().validate(data)

        try:
            manifest = models.Manifest.objects.get(digest=new_data['digest'])
        except models.Manifest.DoesNotExist:
            raise serializers.ValidationError(
                _("The digest '{}' does not exist in the model '{}'"
                  .format(new_data['digest'], models.Manifest.__name__))
            )

        try:
            RepositoryContent.objects.get(content=manifest, repository=new_data['repository'])
        except RepositoryContent.DoesNotExist:
            raise serializers.ValidationError(
                _("The manifest '{}' does not exist in the provided repository '{}'"
                  .format(manifest, new_data['repository']))
            )

        new_data['manifest'] = manifest
        return new_data


class UnTagImageSerializer(TagOperationSerializer):
    """
    A serializer for parsing and validating data associated with a manifest untagging.
    """

    def validate(self, data):
        """
        Validate data passed through a request call.

        The method checks if the tag exists within the repository.
        """
        new_data = super().validate(data)

        import pydevd_pycharm
        pydevd_pycharm.settrace('localhost', port=12345, stdoutToServer=True, stderrToServer=True)

        tags = models.ManifestTag.objects.filter(name=new_data['tag'])
        if tags.count() == 0:
            raise serializers.ValidationError(
                _("The tag name '{}' does not exist".format(new_data['tag']))
            )

        tagged_manifests = (tag.tagged_manifest for tag in tags)
        contents = RepositoryContent.objects.filter(
            content__in=tagged_manifests,
            repository=new_data['repository']
        )
        if contents.count() == 0:
            raise serializers.ValidationError(
                _("The tag '{}' does not exist in the provided repository '{}'"
                  .format(new_data['tag'], new_data['repository']))
            )

        return new_data
