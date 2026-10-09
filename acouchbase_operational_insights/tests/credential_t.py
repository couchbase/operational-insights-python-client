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


from __future__ import annotations

import pathlib
import shutil
import ssl
import subprocess
from base64 import b64encode
from typing import Any, Tuple

import pytest
from httpx2 import Request

from acouchbase_operational_insights.protocol._core.client_adapter import _AsyncClientAdapter
from couchbase_operational_insights.credential import Credential
from couchbase_operational_insights.protocol._core.auth import DynamicCredentialAuth

_SAMPLE_JWT = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.payload.signature'
_PKCS12_PASSWORD = 'pycboi-test-pw'


@pytest.fixture(scope='session')
def pkcs12_path(cert_paths: Tuple[str, str], tmp_path_factory: pytest.TempPathFactory) -> str:
    """Bundle the session cert + key into a password-encrypted PKCS#12 file."""
    if shutil.which('openssl') is None:
        pytest.skip('openssl CLI not available; skipping PKCS#12 tests')
    cert, key = cert_paths
    tmp = tmp_path_factory.mktemp('mtls-pkcs12-async')
    p12 = tmp / 'client.p12'
    subprocess.run(
        [
            'openssl',
            'pkcs12',
            '-export',
            '-out',
            str(p12),
            '-inkey',
            key,
            '-in',
            cert,
            '-passout',
            f'pass:{_PKCS12_PASSWORD}',
        ],
        check=True,
        capture_output=True,
    )
    return str(p12)


@pytest.fixture(scope='session')
def cert_paths(tmp_path_factory: pytest.TempPathFactory) -> Tuple[str, str]:
    """Self-signed PEM cert + key for mTLS unit tests.

    The tests don't need a real CA, just PEM that ``ssl.SSLContext.load_cert_chain``
    can parse.  Skipped if the ``openssl`` CLI isn't on PATH.
    """
    if shutil.which('openssl') is None:
        pytest.skip('openssl CLI not available; skipping mTLS cert tests')
    tmp = tmp_path_factory.mktemp('mtls-certs-async')
    cert = tmp / 'client.crt'
    key = tmp / 'client.key'
    subprocess.run(
        [
            'openssl',
            'req',
            '-x509',
            '-newkey',
            'rsa:2048',
            '-keyout',
            str(key),
            '-out',
            str(cert),
            '-days',
            '1',
            '-nodes',
            '-subj',
            '/CN=pycboi-test-client',
        ],
        check=True,
        capture_output=True,
    )
    return str(cert), str(key)


def _authorization_header(client: Any) -> str:
    auth = DynamicCredentialAuth(client.credential_holder)
    request_url = f'{client.connection_details.url.get_formatted_url()}{client.operational_insights_path}'
    req = Request('POST', request_url)
    flow = auth.auth_flow(req)
    dispatched = next(flow)
    return str(dispatched.headers.get('Authorization', ''))


class CredentialTestSuite:
    TEST_MANIFEST = [
        'test_jwt_credential_creation',
        'test_jwt_credential_strips_token',
        'test_jwt_credential_rejects_non_string',
        'test_credential_direct_construction_with_jwt',
        'test_credential_direct_construction_with_password',
        'test_credential_direct_construction_with_certificate',
        'test_credential_rejects_unknown_kwargs',
        'test_credential_rejects_mixed_kwargs',
        'test_credential_hides_internal_details',
        'test_credential_from_callable_with_jwt',
        'test_jwt_credential_repr_redacts_token',
        'test_jwt_credential_rejects_http_endpoint',
        'test_jwt_credential_accepts_https_endpoint',
        'test_password_credential_http_authorization_header',
        'test_password_credential_repr_redacts_password',
        'test_certificate_credential_creation',
        'test_certificate_credential_rejects_nonexistent_path',
        'test_certificate_credential_rejects_non_string_cert_path',
        'test_certificate_credential_rejects_non_string_key_path',
        'test_certificate_credential_repr',
        'test_certificate_credential_rejects_http_endpoint',
        'test_certificate_credential_accepts_https_endpoint',
        'test_pkcs12_credential_creation',
        'test_pkcs12_credential_repr_redacts_password',
        'test_pkcs12_wrong_password_fails',
        'test_pkcs12_credential_accepts_https_endpoint',
        'test_pkcs12_credential_rejects_http_endpoint',
        'test_set_credential_pem_to_pkcs12_rotation',
        'test_dynamic_auth_sets_header_from_current_credential',
        'test_async_dynamic_auth_sets_header_from_current_credential',
        'test_dynamic_auth_picks_up_rotated_credential',
        'test_async_dynamic_auth_omits_header_for_certificate',
        'test_set_credential_same_type_updates_state',
        'test_set_credential_password_to_jwt_fails',
        'test_set_credential_jwt_to_password_fails',
        'test_set_credential_password_to_certificate_fails',
        'test_set_credential_certificate_to_jwt_fails',
        'test_set_credential_certificate_rotation_rebuilds_client',
        'test_set_credential_certificate_rotation_failure_does_not_change_state',
        'test_set_credential_failure_does_not_change_state',
    ]

    def test_jwt_credential_creation(self) -> None:
        cred = Credential.from_jwt(_SAMPLE_JWT)
        client = _AsyncClientAdapter('https://localhost', cred)
        assert _authorization_header(client) == f'Bearer {_SAMPLE_JWT}'

    def test_jwt_credential_strips_token(self) -> None:
        cred = Credential.from_jwt(f'  {_SAMPLE_JWT}\n')
        client = _AsyncClientAdapter('https://localhost', cred)
        assert _authorization_header(client) == f'Bearer {_SAMPLE_JWT}'

    @pytest.mark.parametrize(
        ('bad_token', 'expected_exc'),
        [
            (12345, TypeError),
            (None, TypeError),
            (b'bytes.jwt.token', TypeError),
            (1.5, TypeError),
            (['a', 'b'], TypeError),
            ('', ValueError),
            ('   \n', ValueError),
        ],
    )
    def test_jwt_credential_rejects_non_string(self, bad_token: object, expected_exc: type) -> None:
        with pytest.raises(expected_exc):
            Credential.from_jwt(bad_token)  # type: ignore[arg-type]

    def test_credential_direct_construction_with_jwt(self) -> None:
        cred = Credential(jwt_token=f'  {_SAMPLE_JWT}\n')
        client = _AsyncClientAdapter('https://localhost', cred)
        assert _authorization_header(client) == f'Bearer {_SAMPLE_JWT}'

    def test_credential_direct_construction_with_password(self) -> None:
        cred = Credential(username='Administrator', password='password')
        client = _AsyncClientAdapter('http://localhost', cred)
        expected = 'Basic ' + b64encode(b'Administrator:password').decode('ascii')
        assert _authorization_header(client) == expected

    def test_credential_direct_construction_with_certificate(self, cert_paths: Tuple[str, str]) -> None:
        cert_path, key_path = cert_paths
        cred = Credential(cert_path=cert_path, key_path=key_path)
        client = _AsyncClientAdapter('https://localhost', cred)
        assert _authorization_header(client) == ''

    def test_credential_rejects_unknown_kwargs(self) -> None:
        with pytest.raises(TypeError, match='unexpected keyword argument'):
            Credential(usernme='Administrator', password='password')  # type: ignore[call-arg]
        with pytest.raises(TypeError, match='unexpected keyword argument'):
            Credential(jwt_token=_SAMPLE_JWT, extra='ignored')  # type: ignore[call-arg]

    def test_credential_rejects_mixed_kwargs(self, cert_paths: Tuple[str, str]) -> None:
        cert_path, key_path = cert_paths
        with pytest.raises(ValueError, match='Cannot provide both'):
            Credential(jwt_token=_SAMPLE_JWT, cert_path=cert_path, key_path=key_path)

    def test_credential_hides_internal_details(self) -> None:
        public_attrs = {name for name in dir(Credential.from_jwt(_SAMPLE_JWT)) if not name.startswith('_')}
        assert public_attrs == {
            'from_callable',
            'from_certificate',
            'from_jwt',
            'from_username_and_password',
        }

    def test_credential_from_callable_with_jwt(self) -> None:
        cred = Credential.from_callable(lambda: Credential.from_jwt(_SAMPLE_JWT))
        client = _AsyncClientAdapter('https://localhost', cred)
        assert _authorization_header(client) == f'Bearer {_SAMPLE_JWT}'

    def test_jwt_credential_repr_redacts_token(self) -> None:
        cred = Credential.from_jwt(_SAMPLE_JWT)
        rendered = repr(cred)
        assert _SAMPLE_JWT not in rendered
        assert '****' in rendered

    def test_jwt_credential_rejects_http_endpoint(self) -> None:
        with pytest.raises(ValueError, match='require a secure'):
            _AsyncClientAdapter('http://localhost', Credential.from_jwt(_SAMPLE_JWT))

    def test_jwt_credential_accepts_https_endpoint(self) -> None:
        client = _AsyncClientAdapter('https://localhost', Credential.from_jwt(_SAMPLE_JWT))
        assert _authorization_header(client) == f'Bearer {_SAMPLE_JWT}'

    def test_password_credential_http_authorization_header(self) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _AsyncClientAdapter('http://localhost', cred)
        expected = 'Basic ' + b64encode(b'Administrator:password').decode('ascii')
        assert _authorization_header(client) == expected

    def test_password_credential_repr_redacts_password(self) -> None:
        cred = Credential.from_username_and_password('Administrator', 'super-secret')
        rendered = repr(cred)
        assert 'super-secret' not in rendered
        assert '****' in rendered
        assert 'Administrator' in rendered

    def test_certificate_credential_creation(self, cert_paths: Tuple[str, str]) -> None:
        cert_path, key_path = cert_paths
        cred = Credential.from_certificate(cert_path, key_path)
        assert cred._kind == 'cert'

    def test_certificate_credential_rejects_nonexistent_path(self, tmp_path: pathlib.Path) -> None:
        existing = tmp_path / 'exists'
        existing.write_text('placeholder')
        missing = tmp_path / 'missing'
        with pytest.raises(FileNotFoundError):
            Credential.from_certificate(str(missing), str(existing))
        with pytest.raises(FileNotFoundError):
            Credential.from_certificate(str(existing), str(missing))

    @pytest.mark.parametrize('bad_value', [12345, None])
    def test_certificate_credential_rejects_non_string_cert_path(self, bad_value: object) -> None:
        with pytest.raises((TypeError, ValueError)):
            Credential.from_certificate(bad_value, '/tmp/key.pem')  # type: ignore[arg-type]

    def test_certificate_credential_rejects_non_string_key_path(self) -> None:
        # key_path=None is now a valid PKCS#12 signature, so only non-str/non-None
        # values are rejected.
        with pytest.raises(TypeError):
            Credential.from_certificate('/tmp/cert.pem', 12345)  # type: ignore[arg-type]

    def test_certificate_credential_repr(self, cert_paths: Tuple[str, str]) -> None:
        cert_path, key_path = cert_paths
        cred = Credential.from_certificate(cert_path, key_path)
        rendered = repr(cred)
        assert 'Credential(cert_path=' in rendered
        assert 'key_path=' in rendered

    def test_certificate_credential_rejects_http_endpoint(self, cert_paths: Tuple[str, str]) -> None:
        cert_path, key_path = cert_paths
        with pytest.raises(ValueError, match='require a secure'):
            _AsyncClientAdapter('http://localhost', Credential.from_certificate(cert_path, key_path))

    def test_certificate_credential_accepts_https_endpoint(self, cert_paths: Tuple[str, str]) -> None:
        cert_path, key_path = cert_paths
        client = _AsyncClientAdapter('https://localhost', Credential.from_certificate(cert_path, key_path))
        assert client.connection_details.ssl_context is not None

    def test_pkcs12_credential_creation(self, pkcs12_path: str) -> None:
        cred = Credential.from_certificate(pkcs12_path, password=_PKCS12_PASSWORD)
        # PKCS#12 still maps to _kind == 'cert'; only the storage shape differs.
        assert cred._kind == 'cert'
        assert cred._key_path is None

    def test_pkcs12_credential_repr_redacts_password(self, pkcs12_path: str) -> None:
        cred = Credential.from_certificate(pkcs12_path, password=_PKCS12_PASSWORD)
        rendered = repr(cred)
        assert _PKCS12_PASSWORD not in rendered
        assert 'cert_password=****' in rendered

    def test_pkcs12_wrong_password_fails(self, pkcs12_path: str) -> None:
        cred = Credential.from_certificate(pkcs12_path, password='wrong-password')
        # The decode happens lazily when the SSL context is built, i.e. inside
        # _AsyncClientAdapter init via validate_security_options.
        with pytest.raises(ValueError, match='Failed to load PKCS#12 file'):
            _AsyncClientAdapter('https://localhost', cred)

    def test_pkcs12_credential_accepts_https_endpoint(self, pkcs12_path: str) -> None:
        cred = Credential.from_certificate(pkcs12_path, password=_PKCS12_PASSWORD)
        client = _AsyncClientAdapter('https://localhost', cred)
        assert client.connection_details.ssl_context is not None

    def test_pkcs12_credential_rejects_http_endpoint(self, pkcs12_path: str) -> None:
        cred = Credential.from_certificate(pkcs12_path, password=_PKCS12_PASSWORD)
        with pytest.raises(ValueError, match='require a secure'):
            _AsyncClientAdapter('http://localhost', cred)

    @pytest.mark.anyio
    async def test_set_credential_pem_to_pkcs12_rotation(self, cert_paths: Tuple[str, str], pkcs12_path: str) -> None:
        # PEM and PKCS#12 are both _kind == 'cert', so cross-format rotation is
        # allowed.  Both forms should rebuild the client + SSL context.
        cert_path, key_path = cert_paths
        client = _AsyncClientAdapter('https://localhost', Credential.from_certificate(cert_path, key_path))
        await client.create_client()
        first_client = client.client
        first_ssl = client.connection_details.ssl_context
        try:
            await client.update_credential(Credential.from_certificate(pkcs12_path, password=_PKCS12_PASSWORD))
            assert client.client is not first_client
            assert client.connection_details.ssl_context is not first_ssl
            assert client.credential_holder.credential._key_path is None  # now PKCS#12-shaped
        finally:
            await client.close_client()

    def test_dynamic_auth_sets_header_from_current_credential(self) -> None:
        cred = Credential.from_jwt(_SAMPLE_JWT)
        client = _AsyncClientAdapter('https://localhost', cred)
        auth = DynamicCredentialAuth(client.credential_holder)

        req = Request('POST', 'https://localhost/api/v1/request')
        flow = auth.auth_flow(req)
        dispatched = next(flow)
        assert dispatched.headers['Authorization'] == f'Bearer {_SAMPLE_JWT}'

    @pytest.mark.anyio
    async def test_async_dynamic_auth_sets_header_from_current_credential(self) -> None:
        cred = Credential.from_jwt(_SAMPLE_JWT)
        client = _AsyncClientAdapter('https://localhost', cred)
        auth = DynamicCredentialAuth(client.credential_holder)

        request_url = f'{client.connection_details.url.get_formatted_url()}{client.operational_insights_path}'
        req = Request('POST', request_url)
        flow = auth.async_auth_flow(req)
        dispatched = await flow.__anext__()
        assert dispatched.headers['Authorization'] == f'Bearer {_SAMPLE_JWT}'

    @pytest.mark.anyio
    async def test_dynamic_auth_picks_up_rotated_credential(self) -> None:
        cred = Credential.from_jwt(_SAMPLE_JWT)
        client = _AsyncClientAdapter('https://localhost', cred)
        auth = DynamicCredentialAuth(client.credential_holder)

        new_token = 'rotated.jwt.token'
        await client.update_credential(Credential.from_jwt(new_token))

        req = Request('POST', 'https://localhost/api/v1/request')
        flow = auth.auth_flow(req)
        dispatched = next(flow)
        assert dispatched.headers['Authorization'] == f'Bearer {new_token}'

    @pytest.mark.anyio
    async def test_async_dynamic_auth_omits_header_for_certificate(self, cert_paths: Tuple[str, str]) -> None:
        cert_path, key_path = cert_paths
        cred = Credential.from_certificate(cert_path, key_path)
        client = _AsyncClientAdapter('https://localhost', cred)
        auth = DynamicCredentialAuth(client.credential_holder)

        req = Request('POST', 'https://localhost/api/v1/request')
        flow = auth.async_auth_flow(req)
        dispatched = await flow.__anext__()
        assert 'Authorization' not in dispatched.headers

    @pytest.mark.anyio
    async def test_set_credential_same_type_updates_state(self) -> None:
        cred = Credential.from_jwt(_SAMPLE_JWT)
        client = _AsyncClientAdapter('https://localhost', cred)
        assert _authorization_header(client) == f'Bearer {_SAMPLE_JWT}'

        new_token = 'fresh.jwt.token'
        await client.update_credential(Credential.from_jwt(new_token))

        assert _authorization_header(client) == f'Bearer {new_token}'

    @pytest.mark.anyio
    async def test_set_credential_password_to_jwt_fails(self) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _AsyncClientAdapter('http://localhost', cred)
        with pytest.raises(TypeError, match='Cannot switch credential type'):
            await client.update_credential(Credential.from_jwt(_SAMPLE_JWT))

    @pytest.mark.anyio
    async def test_set_credential_jwt_to_password_fails(self) -> None:
        cred = Credential.from_jwt(_SAMPLE_JWT)
        client = _AsyncClientAdapter('https://localhost', cred)
        with pytest.raises(TypeError, match='Cannot switch credential type'):
            await client.update_credential(Credential.from_username_and_password('Administrator', 'password'))

    @pytest.mark.anyio
    async def test_set_credential_password_to_certificate_fails(self, cert_paths: Tuple[str, str]) -> None:
        cert_path, key_path = cert_paths
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _AsyncClientAdapter('http://localhost', cred)
        with pytest.raises(TypeError, match='Cannot switch credential type'):
            await client.update_credential(Credential.from_certificate(cert_path, key_path))

    @pytest.mark.anyio
    async def test_set_credential_certificate_to_jwt_fails(self, cert_paths: Tuple[str, str]) -> None:
        cert_path, key_path = cert_paths
        cred = Credential.from_certificate(cert_path, key_path)
        client = _AsyncClientAdapter('https://localhost', cred)
        await client.create_client()
        try:
            with pytest.raises(TypeError, match='Cannot switch credential type'):
                await client.update_credential(Credential.from_jwt(_SAMPLE_JWT))
        finally:
            await client.close_client()

    @pytest.mark.anyio
    async def test_set_credential_certificate_rotation_rebuilds_client(self, cert_paths: Tuple[str, str]) -> None:
        cert_path, key_path = cert_paths
        cred = Credential.from_certificate(cert_path, key_path)
        client = _AsyncClientAdapter('https://localhost', cred)
        await client.create_client()
        first_client = client.client
        first_ssl = client.connection_details.ssl_context
        try:
            await client.update_credential(Credential.from_certificate(cert_path, key_path))
            assert client.client is not first_client
            assert client.connection_details.ssl_context is not first_ssl
        finally:
            await client.close_client()

    @pytest.mark.anyio
    async def test_set_credential_certificate_rotation_failure_does_not_change_state(
        self, cert_paths: Tuple[str, str], tmp_path: pathlib.Path
    ) -> None:
        cert_path, key_path = cert_paths
        cred = Credential.from_certificate(cert_path, key_path)
        client = _AsyncClientAdapter('https://localhost', cred)
        await client.create_client()
        first_client = client.client
        first_ssl = client.connection_details.ssl_context
        bad_cert = tmp_path / 'bad.crt'
        bad_key = tmp_path / 'bad.key'
        bad_cert.write_text('not a cert')
        bad_key.write_text('not a key')
        try:
            with pytest.raises(ssl.SSLError):
                await client.update_credential(Credential.from_certificate(str(bad_cert), str(bad_key)))
            assert client.credential_holder.credential is cred
            assert client.client is first_client
            assert client.connection_details.ssl_context is first_ssl
        finally:
            await client.close_client()

    @pytest.mark.anyio
    async def test_set_credential_failure_does_not_change_state(self) -> None:
        cred = Credential.from_username_and_password('Administrator', 'password')
        client = _AsyncClientAdapter('http://localhost', cred)
        original_header = _authorization_header(client)

        with pytest.raises(TypeError):
            await client.update_credential(Credential.from_jwt(_SAMPLE_JWT))

        assert _authorization_header(client) == original_header


class CredentialTests(CredentialTestSuite):
    @pytest.fixture(scope='class', autouse=True)
    def validate_test_manifest(self) -> None:
        def valid_test_method(meth: str) -> bool:
            attr = getattr(CredentialTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')

        method_list = [meth for meth in dir(CredentialTests) if valid_test_method(meth)]
        test_list = set(CredentialTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')
