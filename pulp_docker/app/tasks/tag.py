from pulpcore.plugin.models import Repository, RepositoryVersion, ContentArtifact, CreatedResource
from pulp_docker.app.models import ManifestTag, Manifest


def tag_image(manifest_pk, tag, repository_pk):
    """
    Create a new repository version out of the passed tag name and the manifest.

    If the tag name is already associated with an existing manifest with the same digest,
    no new content is created. Note that a same tag name cannot be used for two different
    manifests. Due to this fact, an old ManifestTag object is going to be removed from
    a new repository version when a manifest contains a digest which is not equal to the
    digest passed with POST request.
    """
    manifest = Manifest.objects.get(pk=manifest_pk)
    artifact = manifest._artifacts.all()[0]

    existing_manifest_tag_list = ManifestTag.objects.filter(name=tag).exclude(
        tagged_manifest=manifest
    )

    manifest_tag, _ = ManifestTag.objects.get_or_create(
        name=tag,
        tagged_manifest=manifest
    )

    resource = CreatedResource(content_object=manifest_tag)
    resource.save()

    ContentArtifact.objects.get_or_create(
        artifact=artifact,
        content=manifest_tag,
        relative_path=tag
    )

    manifest_tag_list = ManifestTag.objects.filter(pk=manifest_tag.pk)

    repository = Repository.objects.get(pk=repository_pk)

    with RepositoryVersion.create(repository) as repository_version:
        repository_version.remove_content(existing_manifest_tag_list)
        repository_version.add_content(manifest_tag_list)
