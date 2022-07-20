



class User:

    def __init__(self, knex, email):
        self.knex = knex
        self.email = email

    @property
    def username(self):
        return self.email

    # New endpoint has to be created on server and registration can be done with clerk backend API
    # But login cannot be done like that

    # def register(self):
    #     response = requests.post(
    #         f'{self.knex.endpoint_url}/auth/users/',
    #         headers=self.knex.headers,
    #         json={
    #             'email': self.username,
    #             'password': self.password,
    #         }
    #     )
    #     response.raise_for_status()
    #     self.login()
    #     return self
