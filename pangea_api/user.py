
import requests


class User:

    def __init__(self, knex, email, password):
        self.knex = knex
        self.email = email
        self.password = password

    @property
    def username(self):
        return self.email

    def login(self):
        self.knex.login(self.username, self.password)
        return self

    def register(self):
        response = requests.post(
            f'{self.knex.endpoint_url}/auth/users/',
            headers=self.knex.headers,
            json={
                'email': self.username,
                'password': self.password,
            }
        )
        response.raise_for_status()
        self.login()
        return self
