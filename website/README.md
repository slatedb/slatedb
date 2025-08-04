# SlateDB Website

This is the official website for SlateDB, built with [Astro Starlight](https://starlight.astro.build/).

## Development

```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview
```

## Structure

- `src/content/docs/index.astro` - Homepage
- `src/components/` - Reusable Astro components
- `src/content/docs/docs` - Documentation content. Under a second `docs` directory to preserve the original path structure.
- `public/` - Static assets (images, favicon, etc.)

## Components

- `Card.astro` - Custom cards with SlateDB logos and custom sizes
- `Footer.astro` - Site footer with copyright and site title
- `Header.astro` - Site header with logo, search bar, social icons and menu
- `Hero.astro` - Hero section component for homepage
- `PageFrame.astro` - Custom frame for all pages that include the global footer
- `SocialIcons.astro` - Social media links with hover effects
- `ThemeProvider.astro` - Theme context provider component to enforce dark mode

## Styling

The website uses semantic HTML and CSS with:
- No complex CSS frameworks
- Responsive design with CSS Grid and Flexbox
- Consistent color scheme and typography
- Accessible design patterns

## Contributing

To contribute to the website:

1. Make changes to the Astro components or content
2. Test locally with `npm run dev`
3. Build to check for errors: `npm run build`
4. Submit a pull request

### Adding code examples

If you're going to add a code example, we encourage you to add it to [SlateDB's examples collection](../slatedb-examples/). That way, the example is fully compiled with our tests, and we prevent it going out of sync.

Once you have a full executable in that directory, you can load the code in the documentation using Astro's Code component:

1. Ensure that the documentation page is **MDX**, for example _quickstart.mdx_.
2. Import the `Code` component if it's not imported in the page already:
    ```js
    import { Code } from '@astro/starlight/components'
    ```
3. Import the raw code example (notice that the path starts with `/../` to make it relative to the `website` directory):
    ```js
    import example from '/../slatedb-examples/src/my_example.rs?raw'
    ```
4. Render the code:
    ```js
    <Code code={example} lang="rust" title="main.rs" />
    ```
