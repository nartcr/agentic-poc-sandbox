import unittest
from unittest.mock import patch, MagicMock


class TestHandler(unittest.TestCase):

    @patch("handler.process_event")
    @patch("handler.load_config")
    def test_happy_path_returns_200(self, mock_load_config, mock_process_event):
        # LOGIC — valid invocation should return 200 with body
        mock_load_config.return_value = {"secret_name": "x", "credentials": {}}
        mock_process_event.return_value = {"status": "ok", "processed_at": "2024-01-01T12:00:00-05:00"}

        from handler import handler
        response = handler({"key": "value"})

        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(response["body"]["status"], "ok")
        mock_load_config.assert_called_once()
        mock_process_event.assert_called_once()

    @patch("handler.load_config")
    def test_missing_env_var_returns_500(self, mock_load_config):
        # LOGIC — EnvironmentError from config should produce 500
        mock_load_config.side_effect = EnvironmentError("SECRET_NAME environment variable is not set")

        from handler import handler
        response = handler({})

        self.assertEqual(response["statusCode"], 500)
        self.assertIn("error", response["body"])

    @patch("handler.process_event")
    @patch("handler.load_config")
    def test_type_error_returns_400(self, mock_load_config, mock_process_event):
        # LOGIC — TypeError from process should produce 400
        mock_load_config.return_value = {}
        mock_process_event.side_effect = TypeError("event must be a dict")

        from handler import handler
        response = handler("bad input")

        self.assertEqual(response["statusCode"], 400)
        self.assertIn("error", response["body"])

    @patch("handler.process_event")
    @patch("handler.load_config")
    def test_unexpected_exception_returns_500(self, mock_load_config, mock_process_event):
        # LOGIC — unexpected exceptions should be caught and return 500
        mock_load_config.return_value = {}
        mock_process_event.side_effect = RuntimeError("something went wrong")

        from handler import handler
        response = handler({"k": "v"})

        self.assertEqual(response["statusCode"], 500)
        self.assertEqual(response["body"]["error"], "internal error")

    @patch("handler.process_event")
    @patch("handler.load_config")
    def test_handler_passes_event_and_config_to_process(self, mock_load_config, mock_process_event):
        # LOGIC — handler must forward the event and config correctly
        config_stub = {"secret_name": "s", "credentials": {"k": "v"}}
        mock_load_config.return_value = config_stub
        mock_process_event.return_value = {"status": "ok"}

        from handler import handler
        event = {"data": 42}
        handler(event)

        mock_process_event.assert_called_once_with(event, config_stub)


if __name__ == "__main__":
    unittest.main()