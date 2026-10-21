<script setup lang="ts">
import { onMounted, onBeforeUnmount, ref, watch } from 'vue';
import { basicSetup, EditorView } from 'codemirror';
import { keymap } from '@codemirror/view';
import { sql } from '@codemirror/lang-sql';
import { autocompletion, type Completion } from '@codemirror/autocomplete';
import type { LakeTable } from '../shared/types';
import { quoteIdentifier } from './queries';

const props = defineProps<{ modelValue: string; tables: LakeTable[]; selected: LakeTable }>();
const emit = defineEmits<{ 'update:modelValue': [value: string]; run: [] }>();
const host = ref<HTMLElement>();
let editor: EditorView;
onMounted(() => {
  const options: Completion[] = [
    ...props.selected.columns.map(c => ({ label: c.name, type: 'property', detail: c.type, apply: quoteIdentifier(c.name), boost: 2 })),
    ...props.tables.map(t => ({ label: t.name, type: 'class', detail: t.title, apply: `lake.${quoteIdentifier(t.schema)}.${quoteIdentifier(t.name)}` })),
    ...['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT', 'COUNT', 'SUM', 'AVG', 'JOIN', 'ON', 'AS', 'DESC', 'ASC', 'WITH'].map(label => ({ label, type: 'keyword' })),
  ];
  editor = new EditorView({
    parent: host.value,
    doc: props.modelValue,
    extensions: [basicSetup, sql(), autocompletion({ override: [context => {
      const word = context.matchBefore(/[\w\u00c0-\u024f-]*/);
      if (!word || (!word.text && !context.explicit)) return null;
      return { from: word.from, options, validFor: /^[\w\u00c0-\u024f-]*$/ };
    }] }), keymap.of([{ key: 'Mod-Enter', run: () => { emit('run'); return true; } }]),
    EditorView.contentAttributes.of({ 'aria-label': 'Consulta SQL', spellcheck: 'false' }),
    EditorView.updateListener.of(update => { if (update.docChanged) emit('update:modelValue', update.state.doc.toString()); }),
    EditorView.theme({
      '&': { backgroundColor: '#f7f9f3', fontSize: '13px', minHeight: '170px' },
      '.cm-content': { fontFamily: 'ui-monospace, monospace', padding: '15px 8px' },
      '.cm-gutters': { backgroundColor: '#f1f5eb', color: '#97a58c', border: 'none' },
      '&.cm-focused': { outline: '1px solid #b8cba9' },
    })],
  });
});
watch(() => props.modelValue, value => { if (editor && value !== editor.state.doc.toString()) editor.dispatch({ changes: { from: 0, to: editor.state.doc.length, insert: value } }); });
onBeforeUnmount(() => editor?.destroy());
</script>
<template><div ref="host" class="sql-editor" /></template>
