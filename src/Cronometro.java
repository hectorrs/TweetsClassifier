public class Cronometro extends Thread {
	public Cronometro(){
		super();
	}

	public void run() {
		int minutos = 0;
		int segundos = 0;
		int horas = 0;
		
		try {
			for(;;){
				if(segundos != 59){
					segundos++;
				}else{
					if(minutos != 59){
						segundos = 0;
						minutos++;
					}else{
						horas++;
						minutos = 0;
						segundos = 0;
					}
				}
				sleep(999);
			}
		}catch(Exception ex){
			System.out.println("Tiempo de ejecución: " + horas + " h " + minutos + " min " + segundos + " seg.");
		}
	}
}