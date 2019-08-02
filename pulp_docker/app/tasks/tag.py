from pulpcore.plugin.models import Repository, RepositoryVersion, ContentArtifact
from pulp_docker.app.models import ManifestTag, Manifest


def create_new_repository_version(manifest_pk, tag, repository_pk):
    manifest = Manifest.objects.get(pk=manifest_pk)
    artifact = manifest._artifacts.all()[0]

    manifest_tag, _ = ManifestTag.objects.get_or_create(
        name=tag,
        tagged_manifest=manifest
    )

    ContentArtifact.objects.get_or_create(
        artifact=artifact,
        content=manifest_tag,
        relative_path=artifact.file.name
    )

    manifest_tag_list = ManifestTag.objects.filter(pk=manifest_tag.pk)

    repository = Repository.objects.get(pk=repository_pk)

    with RepositoryVersion.create(repository) as repository_version:
        repository_version.add_content(manifest_tag_list)
