package hadoop.pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class PigUdf2 extends EvalFunc<Tuple> {

	public PigUdf2() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)      
		      return null;      
			  Integer objective = (Integer)input.get(2);
			  Integer performance = (Integer)input.get(3);
			  int o = objective.intValue();
			  int p = performance.intValue();
			  if(o>0){
			  double percentage = p/o;
			  if(percentage>0.8){
				  
				  return input;
			  }
			  }
			  return null;
	}

}
