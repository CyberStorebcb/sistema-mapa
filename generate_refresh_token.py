import requests
import webbrowser

APP_KEY = "xlylj191f5sfn36"
APP_SECRET = "edxpdt0jda1xhu0"
REDIRECT_URI = "https://localhost/finish"

# 1. Obtenha o código de autorização
auth_url = (
    f"https://www.dropbox.com/oauth2/authorize?client_id={APP_KEY}"
    f"&redirect_uri={REDIRECT_URI}&response_type=code&token_access_type=offline"
)
print("Abra este link no navegador e autorize o app:")
print(auth_url)
webbrowser.open(auth_url)
auth_code = input("Cole aqui o código (code) da URL após autorizar: ").strip()

# 2. Troque o código pelo refresh token
token_url = "https://api.dropbox.com/oauth2/token"
data = {
    "code": auth_code,
    "grant_type": "authorization_code",
    "client_id": APP_KEY,
    "client_secret": APP_SECRET,
    "redirect_uri": REDIRECT_URI,
}
response = requests.post(token_url, data=data)
print("Resposta da API:")
print(response.json())