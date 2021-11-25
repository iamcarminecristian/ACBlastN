package ahocorasick.trie;

import ahocorasick.interval.Interval;
import ahocorasick.interval.Intervalable;
import scala.Serializable;

public class Emit extends Interval implements Intervalable, Serializable {

    private final String keyword;

    public Emit(final int start, final int end, final String keyword) {
        super(start, end);
        this.keyword = keyword;
    }

    public String getKeyword() {
        return this.keyword;
    }

    @Override
    public String toString() {
        return super.toString() + "=" + this.keyword;
    }
}
