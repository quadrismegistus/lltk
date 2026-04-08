import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

export default defineConfig({
  plugins: [svelte()],
  build: {
    outDir: '../static/dist',
    emptyOutDir: true,
    rollupOptions: {
      input: 'src/main.js',
      output: {
        entryFileNames: 'index.js',
        assetFileNames: 'index[extname]',
      },
    },
  },
});
