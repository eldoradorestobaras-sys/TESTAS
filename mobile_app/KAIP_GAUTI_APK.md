# Kaip gauti „Model MI Kasa“ Android `.apk` per 2 minutes (be jokių programų diegimo į PC)

Kadangi šiame kompiuteryje nėra įdiegtų `Node.js`, `Java` ir `Android SDK`, paruošėme automatinį **GitHub Actions** surinkėją (`.github/workflows/build_apk.yml`), kuris nemokamai sukompiliuoja `.apk` debesyje.

### 3 žingsniai:
1. **Įkelkite į GitHub**: įkelkite aplankus `mobile_app` ir `.github` į savo GitHub repozitoriją (pvz. tą pačią arba naują).
2. **Paleiskite surinkimą**: GitHub puslapyje atsidarykite skirtuką **Actions** → kairėje pasirinkite **Build Model MI Kasa APK** → spustelėkite **Run workflow** (arba jis pasileis automatiškai po įkėlimo).
3. **Atsisiųskite `.apk`**: po ~2 minučių paspauskite ant užbaigto darbo → apačioje prie **Artifacts** atsisiųskite **`Model-MI-Kasa-APK`** (`app-debug.apk`), persiųskite į Android telefoną/planšetę ir įdiekite.

### Pirmas paleidimas telefone / planšetėje:
- Viršuje dešinėje paspauskite **⚙️** ir įsitikinkite, kad įrašyti jūsų adresai:
  - **☁️ Cloud**: `https://testas-azgh.onrender.com`
  - **🏠 LAN**: `http://192.168.0.102:8088` *(jūsų kompiuterio su Model MI vietinis IP)*
  - **Režimas**: `Automatinis: Cloud → LAN`
- Tada daugiau nieko junginėti nereikės: jei Cloud veikia — siųs į Cloud; jei Cloud neatsako — akimirksniu peršoks į LAN; jei abu neveikia — išsaugos eilėje ir išsiųs automatiškai atsiradus ryšiui.
