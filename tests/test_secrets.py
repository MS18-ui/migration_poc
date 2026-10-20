"""Local secret screening that runs before any source is sent to a model."""
import unittest

from agentic.secrets import redact_secrets, secret_lines

CASES = {
    'password = "hunter2abc"': "hunter2abc",
    "export DB_PASSWORD=abc12345": "abc12345",
    "sqoop import --connect jdbc:mysql://h/db --username u --password s3cr3tpw --table t": "s3cr3tpw",
    "sqoop import --password=s3cr3tpw": "s3cr3tpw",
    "jdbc:mysql://h/db?user=a&password=Zz9secret": "Zz9secret",
    "<property><name>db.password</name><value>topsecret1</value></property>": "topsecret1",
    'url = "postgres://u:pw1234@host/db"': "pw1234",
    "aws_access_key_id = 'AKIAEXAMPLE123'": "AKIAEXAMPLE123",
}
SAFE = ["sqoop import --password-file /user/x/pw.txt", "passenger_count = 3", "x = 1",
        "<property><name>queue.name</name><value>default</value></property>", "token_count = len(tokens)"]


class Redaction(unittest.TestCase):
    def test_secrets_are_masked_and_counted(self):
        for text, secret in CASES.items():
            with self.subTest(text=text):
                out, n = redact_secrets(text)
                self.assertGreaterEqual(n, 1)
                self.assertNotIn(secret, out)
                self.assertIn("<REDACTED>", out)

    def test_references_and_ordinary_code_are_untouched(self):
        for text in SAFE:
            with self.subTest(text=text):
                self.assertEqual(redact_secrets(text), (text, 0))

    def test_idempotent(self):
        once, _ = redact_secrets("password = 'abcdef'\nexport X_SECRET=zzzzzz")
        self.assertEqual(redact_secrets(once), (once, 0))

    def test_secret_lines_reports_locations_not_values(self):
        self.assertEqual(secret_lines("a = 1\npassword = 'abcdef'\nb = 2"), [2])


if __name__ == "__main__":
    unittest.main()
