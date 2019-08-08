from pulpcore.plugin.models import Repository, RepositoryVersion
from pulp_docker.app.models import ManifestTag


def untag_image(tag, repository_pk):
    """
    Create a new repository version without a specified manifest's tag name.
    """
    manifest_tag_list = ManifestTag.objects.filter(name=tag)

    repository = Repository.objects.get(pk=repository_pk)

    with RepositoryVersion.create(repository) as repository_version:
        repository_version.remove_content(manifest_tag_list)
