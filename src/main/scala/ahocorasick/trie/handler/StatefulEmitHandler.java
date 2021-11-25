package ahocorasick.trie.handler;

import java.util.List;

import ahocorasick.trie.Emit;

public interface StatefulEmitHandler extends EmitHandler {
    List<Emit> getEmits();
}
