from pulpcore.plugin.models import Repository, RepositoryVersion
from pulp_docker.app.models import ManifestTag


def create_new_repository_version(tag, repository_pk):
    manifest_tag_list = ManifestTag.objects.filter(name=tag)

    repository = Repository.objects.get(pk=repository_pk)

    with RepositoryVersion.create(repository) as repository_version:
        repository_version.remove_content(manifest_tag_list)
