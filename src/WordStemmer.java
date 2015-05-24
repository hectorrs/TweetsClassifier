import java.util.Collections;

import weka.core.Stopwords;
import weka.core.stemmers.IteratedLovinsStemmer;

public class WordStemmer {
	private Stopwords sw = null;
	private IteratedLovinsStemmer stemmer = null;
	
	@SuppressWarnings("unchecked")
	public WordStemmer(){
		sw = new Stopwords();
		sw.remove("thank");
		sw.remove("thanks");
		stemmer = new IteratedLovinsStemmer();
		for(Object s : Collections.list(sw.elements())){
			sw.add(stemmer.stem(s.toString()));
		}
	}
	
	public String stem(String word){
		String toret = word;
		toret = toret.replace("'", "");
		toret = toret.replace("\"", "");
		toret = toret.replace("!", "");
		toret = toret.replace("?", "");
		toret = toret.replace(".", "");
		toret = toret.replace(";", "");
		toret = toret.replace(":", "");
		toret = toret.replace(",", "");
		toret = toret.replace("@", "");
		toret = toret.replace("'", "");
		toret = toret.replace("[", "");
		toret = toret.replace("]", "");
		toret = toret.replace("(", "");
		toret = toret.replace(")", "");
		toret = toret.replace("%", "");
		toret = toret.replace("#", "");
		toret = toret.replace("=", "");
		toret = toret.replace("+", "");
		toret = toret.replace("/", "");
		toret = stemmer.stem(toret.toLowerCase());
		
		if(sw.is(toret) || toret.equals("")){
			toret = null;
		}
		
		return toret;
	}
	
	@SuppressWarnings("unchecked")
	public void printAllStopwords(){
		for(Object s : Collections.list(sw.elements())){
			System.out.println(s);
		}
	}
}