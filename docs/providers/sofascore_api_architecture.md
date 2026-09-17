# Arquitectura de Peticiones a SofaScore API y Análisis del Bloqueo 403

Este documento detalla la arquitectura técnica utilizada en el proyecto para comunicarse con la API de SofaScore, incluyendo la gestión de sesiones con `curl_cffi`, TLS fingerprinting, proxies residenciales rotativos, políticas de reintentos y rate limiting. Asimismo, resume el diagnóstico técnico del bloqueo HTTP 403 (Cloudflare Challenge) y las directrices de mitigación.

Referencia de código:
* Cliente API: [`modules/sofascore/client.py`](file:///c:/Users/gadie/Documents/projects/sofascore/modules/sofascore/client.py)
* Proxy Identity Manager: [`infrastructure/network/proxy_manager.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/network/proxy_manager.py)
* Configuración: [`infrastructure/settings/config.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/settings/config.py)
* Documentos relacionados: [`docs/providers/pre_start_odds_ingestion.md`](file:///c:/Users/gadie/Documents/projects/sofascore/docs/providers/pre_start_odds_ingestion.md) y [`docs/sofascore_challenge_handling.md`](file:///c:/Users/gadie/Documents/projects/sofascore/docs/sofascore_challenge_handling.md).

---

## 1. Arquitectura de Peticiones a SofaScore

Todo el tráfico hacia SofaScore está centralizado y gestionado exclusivamente a través de la clase `SofaScoreAPI` ubicada en [`modules/sofascore/client.py`](file:///c:/Users/gadie/Documents/projects/sofascore/modules/sofascore/client.py).

### 1.1. Librería Base y TLS Fingerprinting

El proyecto **no utiliza la librería estándar `requests` de Python**. En su lugar, utiliza **`curl_cffi`**.

* **¿Por qué?** La librería estándar `requests` tiene una firma criptográfica (JA3/TLS fingerprint) fácilmente detectable por los WAFs y sistemas anti-bot como un script automatizado.
* **Impersonation:** `curl_cffi` falsifica el handshake criptográfico TLS para emular exactamente el comportamiento de un navegador moderno. La sesión se inicializa imitando a Google Chrome (`requests.Session(impersonate="chrome136")`).

### 1.2. Cabeceras HTTP (Headers)

Todas las peticiones inyectan un conjunto riguroso de cabeceras de Chromium en entorno Windows:

* `User-Agent`: Chrome moderno en Windows 10/11.
* `Sec-Ch-Ua`, `Sec-Fetch-Dest`, `Sec-Fetch-Mode`, `Sec-Fetch-Site`: Cabeceras estándar de Client Hints requeridas por navegadores actuales.
* `Accept-Language`, `Accept-Encoding`: Configuración representativa de un usuario real.
* `Connection`: `keep-alive` para mantener la sesión TCP activa entre llamadas.

### 1.3. Manejo de Proxies (Decodo Rotating Residential Proxies)

El cliente utiliza proxies residenciales rotativos gestionados mediante `ProxyIdentityManager`:

* **Configuración:** Enrutamiento a través del gateway (`Config.PROXY_HOST:Config.PROXY_PORT`).
* **Rotación por Conexión TCP:** El proveedor asigna una nueva IP residencial por cada conexión TCP iniciada.
* **Sticky Sessions:** Al utilizar un Connection Pool en `curl_cffi`, la IP residencial asignada se mantiene estable durante la vida de la sesión HTTP.
* **Rotación Forzada (`_rotate_proxy_identity`):** Al detectar un error 403 o 429 persistente, el cliente destruye la sesión `curl_cffi` existente e inicializa una nueva. Esto desencadena un nuevo handshake TCP que obtiene inmediatamente una IP residencial fresca y limpia.

### 1.4. Flujo de Reintentos y Excepciones

El método central `_make_request` encapsula todas las llamadas HTTP:

* **HTTP 200 (OK):** Deserializa la respuesta JSON y resetea los contadores de fallas consecutivas.
* **HTTP 404 (Not Found):** Se detiene sin reintentos innecesarios y levanta `SofaScoreNotFoundException` (o se maneja de forma controlada en la ingesta).
* **HTTP 429 (Rate Limit) y HTTP 403 (Forbidden / Challenge):**
  1. *Backoff Exponencial:* Pausa proporcional al número de intento.
  2. *Rotación de Proxy:* Destruye la sesión actual y solicita una nueva IP residencial.
  3. *Reintento:* Ejecuta la petición nuevamente.
  4. *Exhausted:* Si supera `Config.MAX_RETRIES`, levanta `SofaScoreRateLimitException`.
* **HTTP 5xx (Server Errors):** Reintentos breves asumiendo intermitencia en los servidores del proveedor, sin invalidar la IP residencial.

### 1.5. Rate Limiting Interno

El método `_rate_limit()` implementa un semáforo con `threading.Lock()` y `time.sleep()`. Garantiza que las peticiones a los servidores de SofaScore respeten el intervalo mínimo fijado en `Config.SOFASCORE_RATE_LIMIT_DELAY` (o `SOFASCORE_REQUEST_DELAY_SECONDS`), previniendo bloqueos por ráfagas de tráfico.

---

## 2. Diagnóstico del Bloqueo 403 (Cloudflare Challenge)

Tras un prolongado período de operación estable, las peticiones HTTP directas comenzaron a recibir respuestas HTTP 403 con payload:

```json
{"error": {"code": 403, "reason": "challenge" }}
```

### 2.1. Metodología de Diagnóstico

Se realizaron pruebas empíricas estructuradas para aislar el vector de detección:

1. **`scratch/diagnose_403.py`:** Evaluó combinaciones de dominios (`api.sofascore.com` vs `www.sofascore.com`), cabeceras `Referer` y `Origin`, tráfico con/sin proxy residencial, y múltiples endpoints. Todo el tráfico API directo fue interceptado uniformemente por el `challenge`.
2. **`scratch/diagnose_challenge.py`:** Inspeccionó redirecciones, cookies asociadas y evaluó firmas TLS alternativas (`chrome120`, `chrome131`, `chrome133a`, `chrome136`, `safari184`, `firefox133`). La firma TLS por sí sola no superó el desafío.
3. **`scratch/diagnose_warmup.py`:** Probó calentamiento de sesión contra la página principal para recolectar cookies. El análisis del HTML reveló el recurso:
   ```html
   <link rel="preconnect" href="https://challenges.cloudflare.com"/>
   ```
   Confirmó de manera concluyente la presencia de un **Cloudflare Managed JavaScript Challenge**. Dado que las librerías HTTP puras como `curl_cffi` no ejecutan el motor JS de navegación ni renderizan scripts criptográficos en el DOM, no pueden recibir el token `cf_clearance`.
4. **Prueba de Enfoques Open-Source (`Public-Sofascore-API`):** Se probó el uso de dominios espejo (`api.sofascore.app`) reportados en la comunidad. La infraestructura de SofaScore cerró dichos accesos y retornó igualmente el desafío 403.

---

## 3. Conclusión Arquitectónica y Mitigación

El mecanismo de protección WAF de SofaScore ha evolucionado hacia la verificación activa en cliente mediante retos de JavaScript gestionados por Cloudflare.

Las implicaciones arquitectónicas son:
1. **Peticiones HTTP puras son insuficientes** para endpoints que requieran autorización interactiva o clearance de Cloudflare.
2. **Estrategia Headless Browser**: Para la resolución de desafíos cuando se requiera bypass de clearance, se debe utilizar un navegador real (Playwright / Chromium headless) que ejecute el script de Cloudflare, recolecte la cookie `cf_clearance` y el token de sesión, y los suministre al Connection Pool de `curl_cffi`.
3. **Diversificación de Proveedores**: El pipeline de ingestión mantiene soporte multi-proveedor (OddsPAPI, OddsPortal), asegurando que el monitoreo y los pilares estadísticos no dependan exclusivamente de los endpoints protegidos de SofaScore.
