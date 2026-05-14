(function(){
  'use strict';

  const APP_VERSION = '20260514-1';
  const tg = window.Telegram?.WebApp;
  const root = document.documentElement;
  let scrollRoot = null;
  let touchStartY = 0;
  let touchStartX = 0;

  window.CHESS_MINIAPP_VERSION = APP_VERSION;

  function px(value){
    const number = Number(value) || 0;
    return `${Math.max(0, number)}px`;
  }

  function setVar(name, value){
    root.style.setProperty(name, value);
  }

  function getInset(source, side){
    return Number(source?.[side]) || 0;
  }

  function updateViewportVars(){
    const viewportHeight = Number(tg?.viewportHeight) || window.innerHeight || root.clientHeight || 0;
    const stableHeight = Number(tg?.viewportStableHeight) || viewportHeight;
    const safeArea = tg?.safeAreaInset || {};
    const contentSafeArea = tg?.contentSafeAreaInset || {};

    setVar('--tg-viewport-height-js', px(viewportHeight));
    setVar('--tg-viewport-stable-height-js', px(stableHeight));
    setVar('--tg-safe-area-top', px(getInset(safeArea, 'top')));
    setVar('--tg-safe-area-right', px(getInset(safeArea, 'right')));
    setVar('--tg-safe-area-bottom', px(getInset(safeArea, 'bottom')));
    setVar('--tg-safe-area-left', px(getInset(safeArea, 'left')));
    setVar('--tg-content-safe-area-top', px(getInset(contentSafeArea, 'top')));
    setVar('--tg-content-safe-area-bottom', px(getInset(contentSafeArea, 'bottom')));

    updateScrollRootHeight();
  }

  function getBodyPadding(){
    const style = window.getComputedStyle(document.body);
    return {
      top: parseFloat(style.paddingTop) || 0,
      bottom: parseFloat(style.paddingBottom) || 0
    };
  }

  function updateScrollRootHeight(){
    if (!document.body) return;
    const stableHeight = Number(tg?.viewportStableHeight) || window.innerHeight || root.clientHeight || 0;
    const padding = getBodyPadding();
    setVar('--miniapp-scroll-root-height', px(stableHeight - padding.top - padding.bottom));
  }

  function callTelegram(method){
    try {
      if (typeof tg?.[method] === 'function') tg[method]();
    } catch (err) {
      console.warn(`Telegram WebApp.${method} failed`, err);
    }
  }

  function initTelegram(){
    if (!tg) return;
    updateViewportVars();
    callTelegram('ready');
    callTelegram('expand');
    callTelegram('disableVerticalSwipes');
    try {
      tg.setHeaderColor?.('#111111');
      tg.setBackgroundColor?.('#111111');
      tg.setBottomBarColor?.('#111111');
    } catch (err) {
      console.warn('Telegram color setup failed', err);
    }
    try {
      tg.onEvent?.('viewportChanged', updateViewportVars);
      tg.onEvent?.('safeAreaChanged', updateViewportVars);
      tg.onEvent?.('contentSafeAreaChanged', updateViewportVars);
    } catch (err) {
      console.warn('Telegram viewport handlers failed', err);
    }
  }

  function installBaseStyles(){
    if (document.getElementById('telegramMiniappStyles')) return;
    const style = document.createElement('style');
    style.id = 'telegramMiniappStyles';
    style.textContent = `
      html.tg-miniapp-ready,
      html.tg-miniapp-ready body{
        height:var(--tg-viewport-stable-height-js, 100dvh);
        min-height:var(--tg-viewport-stable-height-js, 100dvh);
        overflow:hidden;
      }
      html.tg-miniapp-ready body{
        position:fixed;
        inset:0;
        width:100%;
      }
      html.tg-miniapp-ready .miniapp-scroll-root{
        height:var(--miniapp-scroll-root-height, auto);
        max-height:var(--miniapp-scroll-root-height, none);
        overflow-x:hidden;
        overflow-y:auto;
        overscroll-behavior-y:contain;
        -webkit-overflow-scrolling:touch;
        touch-action:pan-y;
      }
      html.tg-miniapp-ready .miniapp-scroll-root.is-scroll-locked{
        overflow:hidden;
      }
    `;
    document.head.appendChild(style);
  }

  function markScrollRoot(){
    scrollRoot = document.querySelector('[data-miniapp-scroll-root]')
      || document.querySelector('main.page, main.menu-shell, body > .wrap, body > .page, body > .menu-shell');
    if (!scrollRoot) return;
    scrollRoot.classList.add('miniapp-scroll-root');
    updateScrollRootHeight();
  }

  function isLocalUrl(url){
    return url.origin === window.location.origin && !url.hash.startsWith('#');
  }

  function shouldVersionPath(pathname){
    return /\.(html|css|js)$/i.test(pathname);
  }

  function withVersion(value){
    if (!value || value.startsWith('#') || value.startsWith('mailto:') || value.startsWith('tel:')) return value;
    try {
      const url = new URL(value, window.location.href);
      if (!isLocalUrl(url) || !shouldVersionPath(url.pathname)) return value;
      url.searchParams.set('v', APP_VERSION);
      if (/^[a-z][a-z0-9+.-]*:\/\//i.test(value) || value.startsWith('/')){
        return `${url.pathname}${url.search}${url.hash}`;
      }
      const relativePath = value.split('#')[0].split('?')[0];
      return `${relativePath}${url.search}${url.hash}`;
    } catch (err) {
      return value;
    }
  }

  function versionElement(el){
    if (!el || el.dataset?.miniappVersioned === '1') return;
    const attr = el.tagName === 'A' || el.tagName === 'LINK' ? 'href' : 'src';
    const value = el.getAttribute(attr);
    const versioned = withVersion(value);
    if (versioned !== value) el.setAttribute(attr, versioned);
    if (el.dataset) el.dataset.miniappVersioned = '1';
  }

  function versionLocalResources(scope){
    (scope || document).querySelectorAll?.('a[href], link[href], script[src], iframe[src]').forEach(versionElement);
  }

  function observeLocalResources(){
    const observer = new MutationObserver((mutations) => {
      mutations.forEach((mutation) => {
        mutation.addedNodes.forEach((node) => {
          if (node.nodeType !== 1) return;
          if (node.matches?.('a[href], link[href], script[src], iframe[src]')) versionElement(node);
          versionLocalResources(node);
        });
      });
    });
    observer.observe(document.documentElement, { childList: true, subtree: true });
  }

  function getScrollableElement(target){
    let node = target;
    while (node && node !== document.body && node !== document.documentElement){
      if (node instanceof HTMLElement){
        const style = window.getComputedStyle(node);
        const canScrollY = /(auto|scroll)/.test(style.overflowY) && node.scrollHeight > node.clientHeight + 1;
        if (canScrollY) return node;
      }
      node = node.parentElement;
    }
    return scrollRoot;
  }

  function installTouchGuard(){
    if (!scrollRoot) return;
    scrollRoot.addEventListener('touchstart', (event) => {
      touchStartY = event.touches?.[0]?.clientY || 0;
      touchStartX = event.touches?.[0]?.clientX || 0;
    }, { passive: true });

    scrollRoot.addEventListener('touchmove', (event) => {
      if (event.target?.closest?.('.board, .analysis-frame')) return;
      const currentY = event.touches?.[0]?.clientY || touchStartY;
      const currentX = event.touches?.[0]?.clientX || touchStartX;
      const deltaY = currentY - touchStartY;
      const deltaX = currentX - touchStartX;

      // Keep horizontal swipe/scroll available for tables and carousels.
      if (Math.abs(deltaX) > Math.abs(deltaY)) return;

      const scroller = getScrollableElement(event.target);
      if (!scroller) return;

      const atTop = scroller.scrollTop <= 0;
      const atBottom = scroller.scrollTop + scroller.clientHeight >= scroller.scrollHeight - 1;
      const cannotScroll = scroller.scrollHeight <= scroller.clientHeight + 1;

      if (cannotScroll || (atTop && deltaY > 0) || (atBottom && deltaY < 0)){
        event.preventDefault();
      }
    }, { passive: false });
  }

  function boot(){
    installBaseStyles();
    initTelegram();
    markScrollRoot();
    installTouchGuard();
    versionLocalResources(document);
    observeLocalResources();
    root.classList.add('tg-miniapp-ready');
    window.addEventListener('resize', updateViewportVars, { passive: true });
    window.addEventListener('orientationchange', updateViewportVars, { passive: true });
  }

  if (document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', boot, { once: true });
  } else {
    boot();
  }
})();
