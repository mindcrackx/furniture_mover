from typing import Optional


class Config:
    def __init__(
        self,
        url: str = "http://localhost:5984/",
        user: Optional[str] = None,
        password: Optional[str] = None,
        proxy: Optional[str] = None,
        timeout: float = 3,
    ) -> None:
        if not url.endswith("/"):
            self.url = url + "/"
        else:
            self.url = url

        self.user = user
        self.password = password

        self.proxy = proxy

        try:
            float(timeout)
        except ValueError:
            raise ValueError(f"Timeout {timeout} is not alloewd.")
        self.timeout = timeout
