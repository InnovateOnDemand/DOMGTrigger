# 🚀 Checklist de Deployment a UploadTriggerV2

## ANTES DE HACER PUSH A MASTER

### 1. ✅ Crear Function App en Azure Portal
- [ ] Crear nueva Function App: `UploadTriggerV2`
- [ ] Runtime: `.NET 8 (LTS)`
- [ ] App Service Plan: **REUTILIZAR EL PLAN ACTUAL** (sin costos adicionales)
- [ ] Region: La misma que UploadTrigger
- [ ] Esperar 2-3 minutos a que se cree

### 2. ✅ Copiar Variables de Entorno
- [ ] Abrir UploadTriggerV2 → Settings → Configuration
- [ ] Copiar desde UploadTrigger (app antigua):
  - `AzureWebJobsStorage`
  - `BigQueryProjectName`
  - `BigQueryDatasetName`
  - `GOOGLE_CREDENTIALS_JSON`
- [ ] Agregar nueva variable: `FUNCTIONS_WORKER_RUNTIME` = `dotnet-isolated`
- [ ] Click **Save**

### 3. ✅ Obtener Publish Profile
- [ ] En UploadTriggerV2 → Click **Get publish profile** (botón arriba)
- [ ] Guardar archivo `.PublishSettings`
- [ ] Copiar TODO el contenido (es XML)

### 4. ✅ Agregar Secret a GitHub
- [ ] Ir a GitHub → tu repo → Settings → Secrets and variables → Actions
- [ ] **New repository secret**
  - Name: `AZUREAPPSERVICE_PUBLISHPROFILE_V2`
  - Value: [Pega el XML del publish profile]
- [ ] **Add secret**

### 5. ✅ Verificar Workflow
- [ ] El workflow está actualizado a:
  - DOTNET_VERSION: `8.0.x` ✅
  - app-name: `UploadTriggerV2` ✅
  - publish-profile secret: `AZUREAPPSERVICE_PUBLISHPROFILE_V2` ✅

### 6. ✅ Push a Master (TRIGGER AUTOMÁTICO)
```bash
git add .
git commit -m "Migrate to .NET 8 and Facebook API v24, deploy to UploadTriggerV2"
git push origin master
```

### 7. ✅ Monitorear Deploy
- [ ] GitHub → Actions → Ver que la action se ejecute
- [ ] Esperar a que complete (2-3 minutos)
- [ ] Si falla, ver los logs en GitHub Actions

### 8. ✅ Verificar en Azure
- [ ] Azure Portal → UploadTriggerV2 → Log Stream
- [ ] Enviar mensaje de prueba a `extract-queue`
- [ ] Verificar que se procesa correctamente
- [ ] Revisar Application Insights si hay errores

### 9. ✅ Testing de Funcionalidad
- [ ] Probar con dato real del usuario que envió
- [ ] Verificar que BigQuery query funciona
- [ ] Verificar que Facebook API v24 responde correctamente
- [ ] Revisar blobs en Storage

### 10. ✅ Cleanup (Opcional)
- [ ] Cuando todo funcione, puedes pausar/eliminar `UploadTrigger` (app antigua)
- [ ] Validar que no hay dependencias de la app vieja

---

## 📋 Datos a Tener a Mano

| Dato | Valor |
|------|-------|
| App Name Nueva | `UploadTriggerV2` |
| .NET Version | 8.0 (LTS) |
| Runtime | Isolated Worker |
| API Facebook | v24.0 |
| Secret Name | `AZUREAPPSERVICE_PUBLISHPROFILE_V2` |

---

## 🆘 Si Algo Falla

### GitHub Actions falla:
1. Ver logs en GitHub → Actions → Tu workflow
2. Buscar errores específicos
3. Comprobar que secret está correcto

### Deploy funciona pero app no arranca:
1. Ir a Azure Portal → UploadTriggerV2 → Log Stream
2. Buscar errores
3. Verificar variables de entorno: `FUNCTIONS_WORKER_RUNTIME`, `AzureWebJobsStorage`, etc.

### Queue no se procesa:
1. Verificar en Application Insights
2. Revisar que las variables de entorno de BigQuery están presentes
3. Testear manualmente con Storage Explorer

---

**DEADLINE: 10 de febrero 2026** (Facebook API v22 deprecated)
