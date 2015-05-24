import java.io.File;
import java.util.HashMap;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffSaver;

public class InstanceBuilder {
	// Recibe un HashMap con: <docid_@_category_@_word, tfidf> y crea un fichero arff con nombre "filename" con los datos.
	public static void convertToInstances(HashMap<String,Double> data, String filename){
		HashMap<String,Double> posibleClasses = new HashMap<String,Double>();
		HashMap<String,Double> featureWords = new HashMap<String,Double>();
		HashMap<String,String> instanceIds = new HashMap<String,String>();
		
		FastVector atts = new FastVector();
		for(String key : data.keySet()){
			String[] parts = key.split("_@_");
			if(parts.length>=3){
				String docid = parts[0];
				String category = parts[1];
				String word = parts[2];
				   
				posibleClasses.put(parts[1], 1.0);
				featureWords.put(word, 1.0);
				instanceIds.put(docid, category);
			}
		}
		
		for(String w : featureWords.keySet()){
			atts.addElement(new Attribute(w));
		}
		
		FastVector posibleClassesValues = new FastVector();
		for(String c : posibleClasses.keySet()){
			posibleClassesValues.addElement(c);
		}
		
		atts.addElement(new Attribute("@@class@@",posibleClassesValues ));
		
		Instances toret = new Instances("Dataset",atts,0);
		
		//Generate data
		for(String docid : instanceIds.keySet()){
			double[] vals = new double[toret.numAttributes()];
			String category = instanceIds.get(docid);
			for(int i =0; i< vals.length;i++){
				String featureword = toret.attribute(i).name();
				String idInData = docid+"_@_"+category+"_@_"+featureword;
				if(featureword.equalsIgnoreCase("@@class@@")){
					vals[i] = toret.attribute(i).indexOfValue(category);
				}else{
					if(data.containsKey(idInData)){
						vals[i] = data.get(idInData);
					}else{
						vals[i] = 0.0;
					}
				}
			}
			Instance ins = new Instance(1.0,vals);
			toret.add(ins);
		}
		
		try{
			 ArffSaver saver = new ArffSaver();
			 saver.setInstances(toret);
			 saver.setFile(new File(filename));
			 saver.writeBatch();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
