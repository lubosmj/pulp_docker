import logging
import json
import os

from aiohttp import web
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from multidict import MultiDict

from pulpcore.plugin.content import Handler, PathNotResolved
from pulpcore.plugin.models import ContentArtifact
from pulp_docker.app.models import DockerDistribution, Tag
from pulp_docker.app.docker_convert import Converter_s2_to_s1
from pulp_docker.constants import MEDIA_TYPE


log = logging.getLogger(__name__)

v2_headers = MultiDict()
v2_headers['Docker-Distribution-API-Version'] = 'registry/2.0'

CONFIG_BLOB_RAW = '{"architecture":"arm","config":{"Hostname":"","Domainname":"","User":"","AttachStdin":false,"AttachStdout":false,"AttachStderr":false,"Tty":false,"OpenStdin":false,"StdinOnce":false,"Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],"Cmd":["sh"],"ArgsEscaped":true,"Image":"sha256:dd7d664255f5d53114e9b12f5eac45ab86f633ca7ed8f6345f8ce7b9551f7296","Volumes":null,"WorkingDir":"","Entrypoint":null,"OnBuild":null,"Labels":null},"container":"4dcd86c5bcbb0b4ba5bd980fbf2d5938073d2d3d5564f750c7bc2f5b7de6b22f","container_config":{"Hostname":"4dcd86c5bcbb","Domainname":"","User":"","AttachStdin":false,"AttachStdout":false,"AttachStderr":false,"Tty":false,"OpenStdin":false,"StdinOnce":false,"Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],"Cmd":["/bin/sh","-c","#(nop) ","CMD [\\"sh\\"]"],"ArgsEscaped":true,"Image":"sha256:dd7d664255f5d53114e9b12f5eac45ab86f633ca7ed8f6345f8ce7b9551f7296","Volumes":null,"WorkingDir":"","Entrypoint":null,"OnBuild":null,"Labels":{}},"created":"2018-03-01T08:06:05.664692303Z","docker_version":"17.06.2-ce","history":[{"created":"2018-03-01T08:06:05.394880099Z","created_by":"/bin/sh -c #(nop) ADD file:42e5458a07400ccdb13624b5938915852628509ac97df20e60ea81292293683a in / "},{"created":"2018-03-01T08:06:05.664692303Z","created_by":"/bin/sh -c #(nop)  CMD [\\"sh\\"]","empty_layer":true}],"os":"linux","rootfs":{"type":"layers","diff_ids":["sha256:2b2ae7fb5a0c7c95fca1a5d0cc32fc720b4ff47783ccd448b61ae499d89950b9"]}}'


class ArtifactNotFound(Exception):
    """
    The artifact associated with a published-artifact does not exist.
    """

    pass


class Registry(Handler):
    """
    A set of handlers for the Docker v2 API.
    """

    distribution_model = DockerDistribution

    @staticmethod
    async def get_accepted_media_types(request):
        """
        Returns a list of media types from the Accept headers.

        Args:
            request(:class:`~aiohttp.web.Request`): The request to extract headers from.

        Returns:
            List of media types supported by the client.

        """
        accepted_media_types = []
        for header, values in request.raw_headers:
            if header == b'Accept':
                values = [v.strip().decode('UTF-8') for v in values.split(b",")]
                accepted_media_types.extend(values)
        return accepted_media_types

    @staticmethod
    def _base_paths(path):
        """
        Get a list of base paths used to match a distribution.

        Args:
            path (str): The path component of the URL.

        Returns:
            list: Of base paths.

        """
        return [path]

    @staticmethod
    async def _dispatch(path, headers):
        """
        Stream a file back to the client.

        Stream the bits.

        Args:
            path (str): The fully qualified path to the file to be served.
            headers (dict):

        Returns:
            StreamingHttpResponse: Stream the requested content.

        """
        full_headers = MultiDict()

        full_headers['Content-Type'] = headers['Content-Type']
        full_headers['Docker-Content-Digest'] = headers['Docker-Content-Digest']
        full_headers['Docker-Distribution-API-Version'] = 'registry/2.0'
        full_headers['Content-Length'] = os.path.getsize(path)
        full_headers['Content-Disposition'] = 'attachment; filename={n}'.format(
            n=os.path.basename(path))
        file_response = web.FileResponse(path, headers=full_headers)
        return file_response

    @staticmethod
    async def serve_v2(request):
        """
        Handler for Docker Registry v2 root.

        The docker client uses this endpoint to discover that the V2 API is available.
        """
        return web.json_response({}, headers=v2_headers)

    async def tags_list(self, request):
        """
        Handler for Docker Registry v2 tags/list API.
        """
        path = request.match_info['path']
        distribution = self._match_distribution(path)
        tags = {'name': path, 'tags': set()}
        repository_version = distribution.get_repository_version()
        for c in repository_version.content:
            c = c.cast()
            if isinstance(c, Tag):
                tags['tags'].add(c.name)
        tags['tags'] = list(tags['tags'])
        return web.json_response(tags, headers=v2_headers)

    async def get_tag(self, request):
        """
        Match the path and stream either Manifest or ManifestList.

        Args:
            request(:class:`~aiohttp.web.Request`): The request to prepare a response for.

        Raises:
            PathNotResolved: The path could not be matched to a published file.
            PermissionError: When not permitted.

        Returns:
            :class:`aiohttp.web.StreamResponse` or :class:`aiohttp.web.FileResponse`: The response
                streamed back to the client.

        """
        path = request.match_info['path']
        tag_name = request.match_info['tag_name']
        distribution = self._match_distribution(path)
        repository_version = distribution.get_repository_version()
        accepted_media_types = await Registry.get_accepted_media_types(request)

        try:
            tag = Tag.objects.get(
                pk__in=repository_version.content,
                name=tag_name,
            )
        except ObjectDoesNotExist:
            raise PathNotResolved(tag_name)

        #import pydevd_pycharm
        #pydevd_pycharm.settrace('localhost', port=12345, stdoutToServer=True, stderrToServer=True)

        if tag.tagged_manifest.media_type == MEDIA_TYPE.MANIFEST_V1:
            return_media_type = MEDIA_TYPE.MANIFEST_V1_SIGNED
            response_headers = {'Content-Type': return_media_type,
                                'Docker-Content-Digest': tag.tagged_manifest.digest}
            return await Registry.dispatch_tag(tag, response_headers)

        if tag.tagged_manifest.media_type in accepted_media_types:
            return_media_type = tag.tagged_manifest.media_type
            response_headers = {'Content-Type': return_media_type,
                                'Docker-Content-Digest': tag.tagged_manifest.digest}
            return await Registry.dispatch_tag(tag, response_headers)

        # the path should be split by "/", library/busybox (library is namespace, busybox is repository)
        schema, converted, digest = _convert_manifest(tag, accepted_media_types, path)
        if schema is None:
            raise PathNotResolved(tag_name)
        response_headers = {'Content-Type': MEDIA_TYPE.MANIFEST_V1_SIGNED,
                            'Docker-Content-Digest': digest,
                            'Docker-Distribution-API-Version': 'registry/2.0'}
        if not converted:
            return await Registry.dispatch_tag(schema, response_headers)
        # do not use dispatch_tag here because we did not save converted schema1 as an artifact
        return await Registry.dispatch_converted_schema1(schema, response_headers)

    @staticmethod
    async def dispatch_tag(tag, response_headers):
        """
        Finds an artifact associated with a Tag and sends it to the client.

        Args:
            tag: Tag
            response_headers (dict): dictionary that contains the 'Content-Type' header to send
                with the response

        Returns:
            :class:`aiohttp.web.StreamResponse` or :class:`aiohttp.web.FileResponse`: The response
                streamed back to the client.

        """
        try:
            artifact = tag._artifacts.get()
        except ObjectDoesNotExist:
            raise ArtifactNotFound(tag.name)
        else:
            return await Registry._dispatch(os.path.join(settings.MEDIA_ROOT, artifact.file.name),
                                            response_headers)

    @staticmethod
    async def dispatch_converted_schema1(schema, response_headers):
        return web.Response(text=schema, headers=response_headers)

    async def get_by_digest(self, request):
        """
        Return a response to the "GET" action.
        """
        path = request.match_info['path']
        digest = "sha256:{digest}".format(digest=request.match_info['digest'])
        distribution = self._match_distribution(path)
        repository_version = distribution.get_repository_version()
        log.info(digest)
        try:
            ca = ContentArtifact.objects.get(content__in=repository_version.content,
                                             relative_path=digest)
            headers = {'Content-Type': ca.content.cast().media_type,
                       'Docker-Content-Digest': ca.content.cast().digest}
        except ObjectDoesNotExist:
            raise PathNotResolved(path)
        else:
            artifact = ca.artifact
            if artifact:
                return await Registry._dispatch(os.path.join(settings.MEDIA_ROOT,
                                                             artifact.file.name),
                                                headers)
            else:
                return await self._stream_content_artifact(request, web.StreamResponse(), ca)


def _convert_manifest(tag, accepted_media_types, repository):
    schema1_builder = Schema1ManifestBuilder(tag.name, namespace="ignored", repository=repository)
    if tag.tagged_manifest.media_type == MEDIA_TYPE.MANIFEST_V2:
        # convert schema2 to schema1
        config = _get_config_json(tag.tagged_manifest)
        manifest = _get_manifest_json(tag.tagged_manifest)
        schema1_converted, digest = schema1_builder.build(manifest, config)
        return schema1_builder, True, digest
    elif tag.tagged_manifest.media_type == MEDIA_TYPE.MANIFEST_LIST:
        legacy = _get_legacy_manifest(tag)
        if legacy is None:
            return None, None, None
        if legacy.media_type == MEDIA_TYPE.MANIFEST_V2 and legacy.media_type not in accepted_media_types:
            # convert schema2 to schema1
            config = _get_config_json(legacy)
            manifest = _get_manifest_json(legacy)
            schema1_converted, digest = schema1_builder.build(manifest, config)
            return schema1_converted, True, digest
        else:
            # return legacy without conversion
            return legacy, False, legacy.digest


def _get_legacy_manifest(tag):
    ml = tag.tagged_manifest.listed_manifests.all()
    for manifest in ml:
        m = manifest.manifest_lists.first()
        if m.architecture != 'amd64' or m.os != 'linux':
            continue
        return m.manifest_list
    return None


def _get_config_json(manifest):
    config_artifact = manifest.config_blob._artifacts.first()
    return _get_json(config_artifact)


def _get_manifest_json(manifest):
    manifest_artifact = manifest._artifacts.first()
    return _get_json(manifest_artifact)


def _get_json(artifact):
    with open(os.path.join(settings.MEDIA_ROOT, artifact.file.path)) as json_file:
        json_string = json_file.read()
    return json.loads(json_string)


class Schema1ManifestBuilder(object):
    """
    Abstraction around creating new Schema1Manifests.
    """

    def __init__(self, tag, namespace, repository):
        self.tag = tag
        self.namespace = namespace
        self.repository = repository

    def build(self, manifest, config):
        """
        build schema1 + signature
        """
        converter = Converter_s2_to_s1(
            manifest,
            config,
            namespace=self.namespace,
            repository=self.repository,
            tag=self.tag
        )
        if manifest.get("layers"):
            return converter.convert(), manifest.get("layers")[0].get("digest")
        else:
            return converter.convert(), manifest.get("digest")