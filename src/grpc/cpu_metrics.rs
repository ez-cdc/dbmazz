use std::fs;
use std::time::Instant;

/// Tracker de CPU que lee directamente de /proc/[pid]/stat
/// 
/// Este tracker proporciona métricas de CPU consistentes entre:
/// - Bare metal Linux
/// - Docker containers
/// - Kubernetes pods
/// 
/// Funciona leyendo directamente /proc/[pid]/stat y calculando
/// el delta de CPU entre muestras, exactamente como lo hace `ps` y `top`.
pub struct CpuTracker {
    pid: u32,
    last_utime: u64,
    last_stime: u64,
    last_time: Instant,
    clock_ticks: f64,
    initialized: bool,
}

impl CpuTracker {
    /// Crear un nuevo tracker para el proceso actual
    pub fn new() -> Self {
        let pid = std::process::id();
        
        // CLK_TCK es la frecuencia del reloj del sistema (típicamente 100 Hz en Linux)
        // Esto nos permite convertir ticks de CPU a segundos
        let clock_ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) as f64 };
        
        Self {
            pid,
            last_utime: 0,
            last_stime: 0,
            last_time: Instant::now(),
            clock_ticks,
            initialized: false,
        }
    }
    
    /// Lee utime y stime de /proc/[pid]/stat
    /// 
    /// Retorna (utime, stime) en ticks de CPU
    /// utime = tiempo en modo usuario
    /// stime = tiempo en modo kernel
    fn read_cpu_times(&self) -> Option<(u64, u64)> {
        // Leer /proc/[pid]/stat
        // Formato: pid (comm) state ppid pgrp session tty_nr tpgid flags minflt cminflt majflt cmajflt utime stime ...
        let stat = fs::read_to_string(format!("/proc/{}/stat", self.pid)).ok()?;
        
        // El nombre del comando puede contener espacios y paréntesis, así que necesitamos
        // encontrar el último paréntesis de cierre y partir desde ahí
        let stat = stat.trim_end();
        let rpar_pos = stat.rfind(')')?;
        let parts: Vec<&str> = stat[rpar_pos + 1..].split_whitespace().collect();
        
        // Después del paréntesis de cierre:
        // 0=state 1=ppid 2=pgrp 3=session 4=tty_nr 5=tpgid 6=flags
        // 7=minflt 8=cminflt 9=majflt 10=cmajflt
        // 11=utime 12=stime
        if parts.len() < 13 {
            return None;
        }
        
        let utime: u64 = parts[11].parse().ok()?;
        let stime: u64 = parts[12].parse().ok()?;
        
        Some((utime, stime))
    }
    
    /// Obtiene el consumo de CPU en millicores
    /// 
    /// Retorna:
    /// - 1000 millicores = 100% de 1 core
    /// - 500 millicores = 50% de 1 core
    /// - 35 millicores = 3.5% de 1 core
    /// 
    /// El valor es consistente entre Docker y bare metal.
    pub fn get_cpu_millicores(&mut self) -> u64 {
        let Some((utime, stime)) = self.read_cpu_times() else {
            return 0;
        };
        
        let now = Instant::now();
        let elapsed_secs = now.duration_since(self.last_time).as_secs_f64();
        
        // Primera lectura o intervalo muy corto: solo inicializar estado
        if !self.initialized || elapsed_secs < 0.1 {
            self.last_utime = utime;
            self.last_stime = stime;
            self.last_time = now;
            self.initialized = true;
            return 0;
        }
        
        // Calcular delta de ticks de CPU desde la última lectura
        let delta_utime = utime.saturating_sub(self.last_utime);
        let delta_stime = stime.saturating_sub(self.last_stime);
        let delta_ticks = delta_utime + delta_stime;
        
        // Convertir ticks a segundos de CPU usados
        // cpu_seconds = ticks / clock_ticks
        let cpu_seconds = delta_ticks as f64 / self.clock_ticks;
        
        // Calcular millicores
        // millicores = (cpu_seconds / elapsed_seconds) * 1000
        // 
        // Ejemplo: Si el proceso usó 0.2 segundos de CPU en 1.0 segundos de tiempo real:
        // millicores = (0.2 / 1.0) * 1000 = 200 millicores = 20% de 1 core
        let millicores = (cpu_seconds / elapsed_secs) * 1000.0;
        
        // Actualizar estado para la próxima lectura
        self.last_utime = utime;
        self.last_stime = stime;
        self.last_time = now;
        
        // Asegurar que el valor no sea negativo o excesivamente alto debido a errores de lectura
        millicores.max(0.0).min(100000.0) as u64
    }
}

impl Default for CpuTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;
    
    #[test]
    fn test_cpu_tracker_initialization() {
        let tracker = CpuTracker::new();
        assert!(tracker.pid > 0);
        assert!(tracker.clock_ticks > 0.0);
        assert!(!tracker.initialized);
    }
    
    #[test]
    fn test_cpu_tracker_first_read_returns_zero() {
        let mut tracker = CpuTracker::new();
        // Primera lectura debe retornar 0 (aún no hay delta)
        let millicores = tracker.get_cpu_millicores();
        assert_eq!(millicores, 0);
        assert!(tracker.initialized);
    }
    
    #[test]
    fn test_cpu_tracker_measures_cpu() {
        let mut tracker = CpuTracker::new();
        
        // Primera lectura (inicialización)
        let _ = tracker.get_cpu_millicores();
        
        // Esperar un poco para que haya actividad medible
        thread::sleep(Duration::from_millis(100));
        
        // Segunda lectura debe retornar un valor razonable
        let millicores = tracker.get_cpu_millicores();
        
        // El valor debe estar en un rango razonable (0-1000 millicores típicamente)
        // En tests puede ser bajo porque el proceso está idle
        assert!(millicores < 10000, "CPU millicores too high: {}", millicores);
    }
}

