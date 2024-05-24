from fastapi_admin.providers.login import UsernamePasswordProvider

from app.models import Admin


class AuthProvider(UsernamePasswordProvider):
    login_path = '/login'
    logout_path = '/logout'
    template = 'providers/login/login.html'
    login_title = 'Login to your account'
    login_logo_url = ''
    admin_model = Admin

    async def create_user(self, username: str, password: str, **kwargs):
        return await self.admin_model.create(email=username, username=username, password=password, **kwargs)

    async def pre_save_admin(self, _, instance, using_db, update_fields):
        if instance.password:
            await super().pre_save_admin(_, instance, using_db, update_fields)
