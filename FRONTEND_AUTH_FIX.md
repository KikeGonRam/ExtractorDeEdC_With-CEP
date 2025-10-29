# 🔧 Solución al Error 401 en el Frontend

## Problema
El frontend está recibiendo `401 Unauthorized` al intentar acceder a `/solicitudes` y otros endpoints protegidos.

## Causa
El frontend NO está enviando el token JWT en las peticiones HTTP.

## ✅ Cambios Realizados en el Backend

### 1. Mejor manejo de errores JWT
- ✅ Mensajes de error más descriptivos
- ✅ Logging mejorado para debugging
- ✅ Respuestas JSON estructuradas con información útil

### 2. Nuevo endpoint de verificación
```
GET /auth/verify
Header: Authorization: Bearer {token}

Respuesta exitosa (200):
{
  "valid": true,
  "user": {
    "id_usuario": 123,
    "nombre_usuario": "Juan Pérez",
    "token_payload": {...}
  }
}

Respuesta error (401):
{
  "error": "Token JWT requerido",
  "message": "No se proporcionó token de autenticación...",
  "path": "/auth/verify"
}
```

### 3. CORS actualizado
- ✅ Agregado `https://bechapra.com.mx` a los orígenes permitidos
- ✅ `allow_credentials: true` para permitir headers de autenticación

## 🚨 LO QUE DEBE HACER EL FRONTEND

### Problema Actual en el Frontend
```javascript
// ❌ INCORRECTO - No se envía el token
fetch('https://bechapra.com.mx/extractor-api/solicitudes?page=1&page_size=1000')
```

### Solución 1: Agregar el token a todas las peticiones
```javascript
// ✅ CORRECTO
const token = localStorage.getItem('auth_token') || localStorage.getItem('token');

fetch('https://bechapra.com.mx/extractor-api/solicitudes?page=1&page_size=1000', {
  headers: {
    'Authorization': `Bearer ${token}`
    // O alternativamente:
    // 'auth_token': token
  }
})
```

### Solución 2: Crear un wrapper de fetch
```javascript
// Crear una función wrapper
async function fetchWithAuth(url, options = {}) {
  const token = localStorage.getItem('auth_token') || localStorage.getItem('token');
  
  const headers = {
    ...options.headers,
  };
  
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }
  
  return fetch(url, {
    ...options,
    headers
  });
}

// Usar en lugar de fetch normal
const response = await fetchWithAuth('/extractor-api/solicitudes?page=1&page_size=1000');
```

### Solución 3: Verificar token antes de usarlo
```javascript
// Verificar si el token es válido antes de hacer peticiones
async function isTokenValid() {
  try {
    const token = localStorage.getItem('auth_token');
    if (!token) return false;
    
    const response = await fetch('/extractor-api/auth/verify', {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    
    return response.ok;
  } catch {
    return false;
  }
}

// Usar antes de hacer peticiones importantes
if (!await isTokenValid()) {
  // Redirigir al login
  window.location.href = '/login';
  return;
}
```

### Solución 4: Interceptor de Axios (si usan Axios)
```javascript
import axios from 'axios';

// Configurar interceptor
axios.interceptors.request.use(
  config => {
    const token = localStorage.getItem('auth_token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  error => Promise.reject(error)
);

// Interceptor para manejar 401
axios.interceptors.response.use(
  response => response,
  error => {
    if (error.response?.status === 401) {
      // Token expirado o inválido
      localStorage.removeItem('auth_token');
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);
```

## 📋 Checklist para el Frontend

- [ ] Verificar que el token se está guardando en localStorage después del login
- [ ] Agregar el header `Authorization: Bearer {token}` a TODAS las peticiones protegidas
- [ ] Implementar verificación de token expirado
- [ ] Redirigir al login cuando reciban 401
- [ ] Probar con el nuevo endpoint `/auth/verify`
- [ ] Manejar el caso cuando el token no existe (usuario no logueado)

## 🧪 Testing

### Probar el endpoint de verificación
```bash
# Con token válido
curl -H "Authorization: Bearer TU_TOKEN_AQUI" \
  https://bechapra.com.mx/extractor-api/auth/verify

# Sin token (debería retornar 401)
curl https://bechapra.com.mx/extractor-api/auth/verify
```

## 📝 Logs del Backend

El backend ahora registra información útil:
- `[JWT DEBUG] Verificando token...` - Inicio de verificación
- `[JWT DEBUG] Token válido para usuario: 123` - Token OK
- `JWT requerido pero no se encontró token...` - No se envió token
- `Token JWT expirado` - Token expirado
- `Token JWT inválido` - Token corrupto

Revisa los logs del PM2 para debugging:
```bash
pm2 logs extractor-backend --lines 100
```

## 🚀 Deploy

Después de hacer los cambios en el backend:
```bash
cd ~/Extractor/ExtractorDeEdC_With-CEP
git pull
pm2 restart extractor-backend
pm2 logs extractor-backend
```
