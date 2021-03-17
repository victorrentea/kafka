package victor.training.functions;

import java.util.Date;

public class WordCount {
   public final String word;
   public final Long count;
   public final Date start;
   public final Date end;

   public WordCount(String word, Long count, Date start, Date end) {
      this.word = word;
      this.count = count;
      this.start = start;
      this.end = end;
   }
}
