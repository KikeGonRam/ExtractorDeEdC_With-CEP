# 🔧 Solución al Error 504 Gateway Timeout

## 🚨 Problema
El endpoint `/extract-with-cep/{bank}` está devolviendo **504 Gateway Timeout** porque el procesamiento tarda demasiado (especialmente con muchos movimientos que requieren descargar CEPs).

## Error observado:
```
504 (Gateway Time-out)
Failed to execute 'text' on 'Response': body stream already read
```

---

## ✅ Solución 1: Aumentar Timeout en Nginx (URGENTE)

### En el servidor Linux:

```bash
# 1. Editar configuración de nginx
sudo nano /etc/nginx/sites-available/default

# O si usas otro archivo:
sudo nano /etc/nginx/conf.d/default.conf
```

### Agregar estas líneas en el bloque `location`:

```nginx
server {
    # ... otras configuraciones ...
    
    location /extractor-api/ {
        proxy_pass http://localhost:8000/;
        
        # ⭐ TIMEOUTS AUMENTADOS (10 minutos)
        proxy_read_timeout 600s;
        proxy_connect_timeout 600s;
        proxy_send_timeout 600s;
        
        # Headers importantes
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Buffer sizes (opcional, pero recomendado)
        proxy_buffer_size 128k;
        proxy_buffers 4 256k;
        proxy_busy_buffers_size 256k;
    }
}
```

### Aplicar cambios:

```bash
# Verificar que la configuración es válida
sudo nginx -t

# Si todo OK, recargar nginx
sudo systemctl reload nginx

# O reiniciar si es necesario
sudo systemctl restart nginx

# Verificar estado
sudo systemctl status nginx
```

---

## ✅ Solución 2: Aumentar Worker Timeout en Uvicorn/Gunicorn

Si usas **Gunicorn** con Uvicorn workers:

```bash
# En pm2 o en tu script de inicio
gunicorn app:app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --timeout 600 \
  --bind 0.0.0.0:8000
```

Si usas **PM2** directamente con Uvicorn (como ahora):

El timeout ya es manejado por nginx, pero puedes agregar esta variable de entorno:

```bash
# Editar archivo .env o variables de entorno
export UVICORN_TIMEOUT_KEEP_ALIVE=600
```

---

## ✅ Solución 3: Optimizar el Procesamiento de CEPs

### Opción A: Limitar cantidad de CEPs simultáneos

Editar `cep_banxico.py` para procesar en lotes más pequeños o saltar algunos movimientos.

### Opción B: Aumentar velocidad reduciendo timeouts internos

En `cep_banxico.py`, los `wait_for_timeout()` podrían reducirse ligeramente (¡con cuidado de no romper la funcionalidad!).

### Opción C: Variables de entorno para control

Agregar al archivo `.env` en el servidor:

```bash
# Máximo de CEPs a procesar por estado de cuenta
CEP_MAX_JOBS=50

# Modo headless (ya está en 1)
CEP_HEADLESS=1

# Reducir slowmo si está configurado
CEP_SLOWMO=0
```

---

## ✅ Solución 4: Procesamiento Asíncrono (Futuro)

### Implementar un sistema de cola:

1. El endpoint retorna inmediatamente con un `job_id`
2. El procesamiento se hace en background
3. El frontend hace polling al endpoint `/job/{job_id}` para verificar el estado
4. Cuando termina, descarga el archivo

**Esto requiere cambios mayores en frontend y backend.**

---

## 🧪 Verificar que funciona:

### 1. Probar timeout de nginx:

```bash
# Desde tu máquina local
curl -X POST "https://bechapra.com.mx/extractor-api/healthz" \
  -H "Content-Type: application/json"

# Debería responder inmediatamente
```

### 2. Monitorear logs durante procesamiento:

```bash
# En el servidor
pm2 logs extractor-backend --lines 100

# Ver logs de nginx
sudo tail -f /var/log/nginx/access.log
sudo tail -f /var/log/nginx/error.log
```

### 3. Probar con un PDF pequeño primero

Usa un estado de cuenta con pocos movimientos (3-5) para verificar que funciona.

---

## 📊 Estimación de Tiempos

| Movimientos | Tiempo aprox. | ¿Timeout de 60s? |
|------------|---------------|------------------|
| 5          | ~30-60 seg    | ⚠️ Puede fallar  |
| 10         | ~1-2 min      | ❌ Falla         |
| 20         | ~3-5 min      | ❌ Falla         |
| 50+        | ~10+ min      | ❌ Falla         |

**Con timeout de 600s (10 min)**, debería funcionar para la mayoría de casos.

---

## 🚀 Pasos Inmediatos (Ejecutar en orden)

```bash
# 1. SSH al servidor
ssh usuario@srv947731

# 2. Backup de configuración actual
sudo cp /etc/nginx/sites-available/default /etc/nginx/sites-available/default.backup

# 3. Editar configuración
sudo nano /etc/nginx/sites-available/default

# 4. Agregar los timeouts (ver arriba)

# 5. Verificar sintaxis
sudo nginx -t

# 6. Recargar nginx
sudo systemctl reload nginx

# 7. Probar con un PDF
# (usar el frontend)

# 8. Monitorear logs
pm2 logs extractor-backend
```

---

## ❓ Preguntas Frecuentes

**P: ¿Por qué tarda tanto?**  
R: Por cada movimiento, se abre un navegador, se llena un formulario en Banxico, y se descarga un PDF. Con 20 movimientos, son 20 navegadores + formularios + descargas.

**P: ¿Puedo hacerlo más rápido?**  
R: Sí, reduciendo `slowmo`, ajustando timeouts internos, o procesando menos CEPs por estado.

**P: ¿504 vs 500?**  
R: **504** = El proxy cortó la conexión (timeout). **500** = Error en el backend Python.

**P: ¿Qué es mejor, aumentar timeout o hacer async?**  
R: Para **corto plazo**, aumentar timeout. Para **largo plazo**, implementar procesamiento asíncrono con cola de trabajos.

---

## 📝 Notas

- El timeout debe ser **mayor** que el tiempo máximo que puede tardar el procesamiento
- Nginx por defecto tiene timeout de **60 segundos**
- PM2 no tiene timeout por defecto (solo nginx)
- Los timeouts internos de Playwright son solo para operaciones individuales, no afectan el timeout total

---

## ✅ Checklist de Verificación

- [ ] Configuración de nginx actualizada con `proxy_read_timeout 600s`
- [ ] Nginx recargado exitosamente (`sudo nginx -t && sudo systemctl reload nginx`)
- [ ] Probado con PDF de pocos movimientos
- [ ] Probado con PDF de muchos movimientos
- [ ] Logs sin errores de timeout
- [ ] Frontend recibe el archivo ZIP correctamente
