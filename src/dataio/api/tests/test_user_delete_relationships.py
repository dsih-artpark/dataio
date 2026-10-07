import pytest

from dataio.api.database.models import User


# Each of these tables has ON DELETE CASCADE to users(email). Without
# passive_deletes, session.delete(user) makes SQLAlchemy NULL their NOT NULL
# user_email column first, so deleting the user fails before the cascade runs.
@pytest.mark.parametrize(
    "backref_name",
    ["sessions", "oauth_identities", "webauthn_credentials", "api_keys", "chat_sessions", "downloads"],
)
def test_user_child_relationships_leave_deletes_to_the_database(backref_name):
    relationship = User.__mapper__.relationships[backref_name]
    assert relationship.passive_deletes is True
