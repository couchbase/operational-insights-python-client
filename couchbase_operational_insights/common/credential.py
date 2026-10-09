#  Copyright 2016-2025. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Credentials for the Operational Insights SDK."""

from __future__ import annotations

import logging
import os
import ssl
import tempfile
from base64 import b64encode
from typing import TYPE_CHECKING, Callable, Dict, Literal, Optional

from couchbase_operational_insights.common._core.utils import validate_path

if TYPE_CHECKING:
    from httpx2 import Request


logger = logging.getLogger(__name__)


class Credential:
    """A credential for authenticating with a Couchbase Operational Insights endpoint.

    Construct via a factory classmethod or direct keyword arguments::

        Credential(username='Administrator', password='swordfish')
        Credential(jwt_token='eyJ...')
        Credential(cert_path='/path/to/cert.pem', key_path='/path/to/key.pem')
        Credential(cert_path='/path/to/client.p12', cert_password='secret')
        Credential.from_username_and_password('Administrator', 'swordfish')
        Credential.from_jwt('eyJ...')
        Credential.from_certificate('/path/to/cert.pem', '/path/to/key.pem')
        Credential.from_certificate('/path/to/client.p12', password='secret')
        Credential.from_callable(lambda: Credential.from_jwt(get_token()))
    """

    __slots__ = (
        '_kind',
        '_header',
        '_password',
        '_token',
        '_username',
        '_cert_path',
        '_key_path',
        '_cert_password',
    )

    # PEM and PKCS#12 share `_kind = 'cert'` because they're functionally
    # identical from the SDK's point of view: TLS-handshake auth, https-only,
    # client rebuild on rotation.  What differs is how cert + key are packaged
    # on disk.  We tell the two apart by `_key_path`: set for PEM, None for
    # PKCS#12.
    _kind: Literal['password', 'jwt', 'cert']
    _header: Optional[str]
    _password: Optional[str]
    _token: Optional[str]
    _username: Optional[str]
    _cert_path: Optional[str]
    _key_path: Optional[str]
    _cert_password: Optional[str]

    def __init__(
        self,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        jwt_token: Optional[str] = None,
        cert_path: Optional[str] = None,
        key_path: Optional[str] = None,
        cert_password: Optional[str] = None,
    ) -> None:
        self._header = None
        self._password = None
        self._token = None
        self._username = None
        self._cert_path = None
        self._key_path = None
        self._cert_password = None

        has_cert = cert_path is not None or key_path is not None or cert_password is not None

        if jwt_token is not None:
            if username is not None or password is not None or has_cert:
                raise ValueError('Cannot provide both a JWT token and username/password or certificate.')
            if not isinstance(jwt_token, str):
                raise TypeError('The JWT token must be a str.')
            jwt_token = jwt_token.strip()
            if not jwt_token:
                raise ValueError('The JWT token must not be empty.')
            self._kind = 'jwt'
            self._header = f'Bearer {jwt_token}'
            self._token = jwt_token
        elif has_cert:
            if username is not None or password is not None:
                raise ValueError('Cannot provide both certificate and username/password.')
            self._init_certificate(cert_path, key_path, cert_password)
        elif username is not None or password is not None:
            self._init_password(username, password)
        else:
            raise ValueError('Must provide one of: (username + password), jwt_token, or cert_path.')

    def _init_password(self, username: Optional[str], password: Optional[str]) -> None:
        if username is None:
            raise ValueError('Must provide a username.')
        if password is None:
            raise ValueError('Must provide a password.')
        if not isinstance(username, str):
            raise TypeError('The username must be a str.')
        if not isinstance(password, str):
            raise TypeError('The password must be a str.')
        self._kind = 'password'
        self._header = 'Basic ' + b64encode(f'{username}:{password}'.encode()).decode('ascii')
        self._password = password
        self._username = username

    def _init_certificate(
        self,
        cert_path: Optional[str],
        key_path: Optional[str],
        cert_password: Optional[str],
    ) -> None:
        if cert_path is None:
            raise ValueError('Must provide a cert_path.')
        if not isinstance(cert_path, str):
            raise TypeError('The cert_path must be a str.')
        if key_path is not None and not isinstance(key_path, str):
            raise TypeError('The key_path must be a str or None.')
        if cert_password is not None and not isinstance(cert_password, str):
            raise TypeError('The cert_password must be a str or None.')

        self._kind = 'cert'
        self._cert_path = validate_path(cert_path)
        self._key_path = validate_path(key_path) if key_path is not None else None
        self._cert_password = cert_password

    def _asdict(self) -> Dict[str, str]:
        """**INTERNAL**"""
        if self._kind == 'jwt':
            return {'jwt_token': self._token or ''}
        if self._kind == 'cert':
            d: Dict[str, str] = {'cert_path': self._cert_path or ''}
            if self._key_path is not None:
                d['key_path'] = self._key_path
            # Omit cert_password when None so reconstruction round-trips an
            # unencrypted file as unencrypted, not as cert_password=''.
            if self._cert_password is not None:
                d['cert_password'] = self._cert_password
            return d
        return {'username': self._username or '', 'password': self._password or ''}

    @classmethod
    def from_username_and_password(cls, username: str, password: str) -> Credential:
        """Create a :class:`.Credential` from a username and password.

        Args:
            username: The username for the Operational Insights endpoint.
            password: The password for the Operational Insights endpoint.
        """
        if not isinstance(username, str):
            raise TypeError('The username must be a str.')
        if not isinstance(password, str):
            raise TypeError('The password must be a str.')
        return cls(username=username, password=password)

    @classmethod
    def from_jwt(cls, token: str) -> Credential:
        """Create a :class:`.Credential` from a JSON Web Token (JWT).

        The SDK sends an ``Authorization: Bearer <jwt>`` header on every HTTP request.
        JWT credentials may only be used with an ``https://`` endpoint.

        .. note::
            JWT credentials have a limited validity period.  To avoid authentication
            failures, periodically pass a fresh credential via
            :meth:`~couchbase_operational_insights.cluster.Cluster.set_credential`.

        Args:
            token: The JSON Web Token.
        """
        if not isinstance(token, str):
            raise TypeError('The JWT token must be a str.')
        return cls(jwt_token=token)

    @classmethod
    def from_certificate(
        cls,
        cert_path: str,
        key_path: Optional[str] = None,
        *,
        password: Optional[str] = None,
    ) -> Credential:
        """Create a :class:`.Credential` for client-certificate (mTLS) authentication.

        Two file layouts are accepted:

        * **PEM cert + PEM key.** Pass both paths::

              Credential.from_certificate('/path/cert.pem', '/path/key.pem')

          ``password`` decrypts the key file if it's encrypted.

        * **PKCS#12 bundle** (``.p12`` / ``.pfx``). Pass only ``cert_path``::

              Credential.from_certificate('/path/client.p12', password='secret')

          Use ``password=None`` (the default) if the file is unencrypted.

        Authentication happens during the TLS handshake; no HTTP
        ``Authorization`` header is sent.  Cert credentials may only be used
        with an ``https://`` endpoint.

        .. note::
            Rotating a cert credential via
            :meth:`~couchbase_operational_insights.cluster.Cluster.set_credential` rebuilds
            the HTTP client, since the cert is baked into the SSL context.

        Args:
            cert_path: Path to a PEM-encoded certificate (chain) or a PKCS#12
                bundle.
            key_path: Path to the PEM-encoded private key.  ``None`` if
                ``cert_path`` is a PKCS#12 bundle that already contains the key.
            password: Decryption password for the PEM key file or PKCS#12
                bundle.  ``None`` if the file is unencrypted.
        """
        if not isinstance(cert_path, str):
            raise TypeError('The cert_path must be a str.')
        if key_path is not None and not isinstance(key_path, str):
            raise TypeError('The key_path must be a str or None.')
        if password is not None and not isinstance(password, str):
            raise TypeError('The password must be a str or None.')
        return cls(cert_path=cert_path, key_path=key_path, cert_password=password)

    @classmethod
    def from_callable(cls, callback: Callable[[], Credential]) -> Credential:
        """Create a :class:`.Credential` by invoking a callback.

        The callback is invoked once at construction time; it is not retained for
        dynamic credential lookup.  To pick up a refreshed credential later, pass
        a fresh :class:`.Credential` to
        :meth:`~couchbase_operational_insights.cluster.Cluster.set_credential`.

        Args:
            callback: Callback that returns a :class:`.Credential`.

        Example::

            cred = Credential.from_callable(
                lambda: Credential.from_username_and_password(
                    os.environ['USERNAME'], os.environ['PASSWORD']
                )
            )
        """
        return cls(**callback()._asdict())

    def _apply_to_request(self, request: Request) -> None:
        if self._header is not None:
            request.headers['Authorization'] = self._header

    def _apply_to_ssl_context(self, ctx: ssl.SSLContext) -> None:
        # No-op for password/JWT credentials.
        if self._kind != 'cert':
            return
        cert_path = self._cert_path
        if cert_path is None:
            # Unreachable: _init_certificate always sets _cert_path when _kind=='cert'.
            raise RuntimeError('_cert_path is unset for a cert credential.')
        if self._key_path is None:
            # PKCS#12 bundle: cert_path holds the .p12 file.
            self._load_pkcs12_into_context(ctx)
            return
        # PEM cert + PEM key.  password is forwarded to OpenSSL so it can
        # decrypt the key file if it's encrypted; ignored for plain keys.
        ctx.load_cert_chain(
            certfile=cert_path,
            keyfile=self._key_path,
            password=self._cert_password,
        )

    def _load_pkcs12_into_context(self, ctx: ssl.SSLContext) -> None:
        from cryptography.hazmat.primitives.serialization import (
            Encoding,
            NoEncryption,
            PrivateFormat,
            pkcs12,
        )

        cert_path = self._cert_path
        if cert_path is None:
            # Unreachable: _init_certificate always sets _cert_path when _kind=='cert'.
            raise RuntimeError('_cert_path is unset for a PKCS#12 credential.')
        with open(cert_path, 'rb') as f:
            data = f.read()
        password_bytes = self._cert_password.encode('utf-8') if self._cert_password is not None else None
        try:
            key, cert, additional_certs = pkcs12.load_key_and_certificates(data, password_bytes)
        except ValueError as e:
            # cryptography raises ValueError for both wrong password and malformed
            # input.  Wrap with the path so the cluster log identifies which file.
            raise ValueError(f'Failed to load PKCS#12 file at {cert_path}: {e}') from e
        if key is None or cert is None:
            raise ValueError(f'PKCS#12 file at {cert_path} does not contain a private key + certificate pair.')

        # ssl.SSLContext.load_cert_chain only accepts file paths, so we write
        # the chain plus the decrypted key into a single PEM tempfile, hand
        # the path to OpenSSL, and unlink right after.  The unencrypted key is
        # on disk only between the write and the unlink in finally.
        tmp_fd, tmp_path = tempfile.mkstemp(suffix='.pem', prefix='pycboi-pkcs12-')
        try:
            with os.fdopen(tmp_fd, 'wb') as tmp:
                tmp.write(cert.public_bytes(Encoding.PEM))
                for ca in additional_certs or []:
                    tmp.write(ca.public_bytes(Encoding.PEM))
                tmp.write(key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()))
            ctx.load_cert_chain(certfile=tmp_path)
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                logger.warning(
                    'Failed to remove temporary PKCS#12 PEM file %s; '
                    'decrypted private key material may remain on disk.',
                    tmp_path,
                    exc_info=True,
                )

    def _check_endpoint_compatible(self, is_secure: bool) -> None:
        if self._kind == 'jwt' and not is_secure:
            raise ValueError('JWT credentials require a secure (https) connection.')
        if self._kind == 'cert' and not is_secure:
            raise ValueError('Client-certificate credentials require a secure (https) connection.')

    def _check_replaceable_with(self, new_credential: Credential) -> None:
        if self._kind != new_credential._kind:
            raise TypeError(
                f'Cannot switch credential type at runtime; current type is '
                f'{self._kind}, new type is {new_credential._kind}.'
            )

    def __repr__(self) -> str:
        if self._kind == 'password':
            return f'Credential(username={self._username!r}, password=****)'
        if self._kind == 'cert':
            pw = ', cert_password=****' if self._cert_password is not None else ''
            if self._key_path is None:
                return f'Credential(cert_path={self._cert_path!r}{pw})'
            return f'Credential(cert_path={self._cert_path!r}, key_path={self._key_path!r}{pw})'
        return 'Credential(jwt_token=****)'


class _CredentialHolder:
    """**INTERNAL** Owns the mutable current credential; transport calls
    apply_to_request per request and replace for rotation.
    """

    __slots__ = ('_credential',)

    def __init__(self, credential: Credential) -> None:
        self._credential = credential

    @property
    def credential(self) -> Credential:
        return self._credential

    def apply_to_request(self, request: Request) -> None:
        self._credential._apply_to_request(request)

    def replace(self, new_credential: Credential) -> None:
        # Caller must have validated replaceability via _check_replaceable_with;
        # this is a GIL-atomic store, last-writer-wins on concurrent rotators.
        self._credential = new_credential
