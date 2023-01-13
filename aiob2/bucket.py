from typing import Optional, Any

import aiohttp

from .models import File, DeletedFile, DownloadedFile
from .http import HTTPClient, BucketInfo

from .errors import BackblazeException

__all__ = ("Client",)


class Client:
    """Represents an aiob2 Client that makes requests to Backblaze's B2 API.

    Parameters
    ----------
    application_key_id: :class:`str`
        The application key id to use for authentication.
    application_key: :class:`str`
        The application key to use for authentication.
    session: Optional[:class:`aiohttp.ClientSession`]
        An optional session to pass, otherwise one will be lazily created.
    """

    def __init__(
        self,
        application_key_id: str,
        application_key: str,
        *,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self._http = HTTPClient(application_key_id, application_key, session)
        self._bucket_list = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any):
        await self.close()

    async def close(self):
        # This is really only possible if a Client is instantiated and no request is ever made
        if isinstance(self._http._session, aiohttp.ClientSession):
            await self._http._session.close()

    async def list_buckets(self):
        if self._bucket_list is None:
            self._bucket_list = await self._http._get_list_buckets()
        return self._bucket_list

    async def get_bucket_from_name(self, bucket_name: str) -> BucketInfo:
        """Retrieve the bucket id from the bucket name

        Args:
            bucket_name (str): _description_

        Returns:
            str: bucket_id
        """
        bucket_list = await self.list_buckets()

        for bucket in bucket_list:
            if bucket.bucketName == bucket_name:
                return bucket

        # We did not found bucket
        raise BackblazeException(f"Unknown bucket {bucket_name}")

    async def list_file_names(
        self,
        bucket_id: str,
        start_file_name: Optional[str] = None,
        max_file_count: Optional[int] = None,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> list:
        """Lists the names of all files in a bucket, starting at a given name (optional)

        Args:
            bucket_id (str): The ID of the bucket to list
            start_file_name (Optional[str], optional): The first file name to return.
                            If there is a file with this name, it will be returned in the list.
                            If not, the first file name after this the first one after this name..

                            Defaults to None.
            max_file_count (Optional[int], optional): The maximum number of files to return from this call.
                                        The default value is 100. There is no maximum
                                        Passing in 0 means to use the default of 100.
                                        Defaults to None.
            prefix (Optional[str], optional): Files returned will be limited to those with the given prefix. Defaults to None.
            delimiter (Optional[str], optional): Files returned will be limited to those within the top folder, or any one subfolder. Defaults to None.

        Returns:
            list: list of File
        """
        files = []
        continue_iteration = True
        initial_max_file_count = max_file_count
        actual_file_count = 0

        if initial_max_file_count is None:
            initial_max_file_count = 100
        elif initial_max_file_count > 10000:
            max_file_count = 10000
        else:
            max_file_count = initial_max_file_count

        while continue_iteration:
            data = await (
                await self._http.list_file_names(
                    bucket_id=bucket_id,
                    start_file_name=start_file_name,
                    max_file_count=max_file_count,
                    prefix=prefix,
                    delimiter=delimiter,
                )
            )
            files += [File(d) for d in data["files"]]
            start_file_name = data["nextFileName"]

            actual_file_count += len(files)

            if (actual_file_count >= initial_max_file_count) or start_file_name is None:
                continue_iteration = False
            else:
                max_file_count = initial_max_file_count - actual_file_count

        return files

    async def upload_file(
        self,
        *,
        content_bytes: bytes,
        content_type: str,
        file_name: str,
        bucket_id: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ) -> File:
        """Uploads a file to a bucket.

        Parameters
        -----------
        content_bytes: :class:`bytes`
            The raw bytes of the file to be uploaded.
        content_type: :class:`str`
            The content type of the content_bytes, e.g. video/mp4.
        file_name: :class:`str`
            The name of the file.
        bucket_id: Optional[:class:`str`]
            The ID of the bucket to upload to. To privilege if possible
        bucket_name: Optional[:class:`str`]
            The name of the bucket to upload to.

        Returns
        ---------
        :class:`File`
            The uploaded file.
        """
        if bucket_id is None:
           bucket = await self.get_bucket_from_name(bucket_name)
           bucket_id = bucket.bucketId

        data = await (
            await self._http.upload_file(
                content_bytes=content_bytes,
                content_type=content_type,
                file_name=file_name,
                bucket_id=bucket_id,
            )
        )
        return File(data)

    async def delete_file(self, file_name: str, file_id: str) -> DeletedFile:
        """Deletes a file from a bucket.

        Parameters
        -----------
        file_name: :class:`str`
            The name of the file to delete.
        file_id: :class:`str`
            The id of the file to delete.

        Returns
        ---------
        :class:`DeletedFile`
            The deleted file.
        """

        data = await self._http.delete_file(file_name=file_name, file_id=file_id)
        return DeletedFile(data)

    async def download_file_by_id(
        self,
        file_id: str,
        *,
        content_disposition: Optional[str] = None,
        content_language: Optional[str] = None,
        expires: Optional[str] = None,
        cache_control: Optional[str] = None,
        content_encoding: Optional[str] = None,
        content_type: Optional[str] = None,
        server_side_encryption: Optional[str] = None,
    ) -> DownloadedFile:
        """Downloads a file.

        Parameters
        -----------
        file_id: :class:`str`
            The file id of the file to be downloaded.
        content_disposition: Optional[:class:`str`]
            Overrides the current 'b2-content-disposition' specified when the file was uploaded.
        content_language: Optional[:class:`str`]
            Overrides the current 'b2-content-language' specified when the file was uploaded.
        expires: Optional[:class:`str`]
            Overrides the current 'b2-expires' specified when the file was uploaded.
        cache_control: Optional[:class:`str`]
            Overrides the current 'b2-cache-control' specified when the file was uploaded.
        content_encoding: Optional[:class:`str`]
            Overrides the current 'b2-content-encoding' specified when the file was uploaded.
        content_type: Optional[:class:`str`]
            Overrides the current 'Content-Type' specified when the file was uploaded.
        server_side_encryption: Optional[:class:`str`]
            This is requires if the file was uploaded and stored using Server-Side Encryption with
            Customer-Managed Keyts (SSE-C)

        Returns
        ---------
        :class:`DownloadedFile`
            The file requested.
        """

        data = await self._http.download_file_by_id(
            file_id=file_id,
            content_disposition=content_disposition,
            content_language=content_language,
            expires=expires,
            cache_control=cache_control,
            content_encoding=content_encoding,
            content_type=content_type,
            server_side_encryption=server_side_encryption,
        )
        return DownloadedFile(data[0], data[1])  # type: ignore

    async def download_file_by_name(
        self,
        file_name: str,
        bucket_name: str,
        *,
        content_disposition: Optional[str] = None,
        content_language: Optional[str] = None,
        expires: Optional[str] = None,
        cache_control: Optional[str] = None,
        content_encoding: Optional[str] = None,
        content_type: Optional[str] = None,
        server_side_encryption: Optional[str] = None,
    ) -> DownloadedFile:
        """Downloads a file.

        Parameters
        -----------
        file_name: :class:`str`
            The file name of the file to be downloaded.
        bucket_name: :class:`str`
            The bucket name of the file to be downloaded. This should only be specified if you have specified
            file_name and not file_id.
        content_disposition: Optional[:class:`str`]
            Overrides the current 'b2-content-disposition' specified when the file was uploaded.
        content_language: Optional[:class:`str`]
            Overrides the current 'b2-content-language' specified when the file was uploaded.
        expires: Optional[:class:`str`]
            Overrides the current 'b2-expires' specified when the file was uploaded.
        cache_control: Optional[:class:`str`]
            Overrides the current 'b2-cache-control' specified when the file was uploaded.
        content_encoding: Optional[:class:`str`]
            Overrides the current 'b2-content-encoding' specified when the file was uploaded.
        content_type: Optional[:class:`str`]
            Overrides the current 'Content-Type' specified when the file was uploaded.
        server_side_encryption: Optional[:class:`str`]
            This is requires if the file was uploaded and stored using Server-Side Encryption with
            Customer-Managed Keyts (SSE-C)

        Returns
        ---------
        :class:`DownloadedFile`
            The file requested.
        """

        data = await self._http.download_file_by_name(
            file_name=file_name,
            bucket_name=bucket_name,
            content_disposition=content_disposition,
            content_language=content_language,
            expires=expires,
            cache_control=cache_control,
            content_encoding=content_encoding,
            content_type=content_type,
            server_side_encryption=server_side_encryption,
        )
        return DownloadedFile(data[0], data[1])  # type: ignore
