(function(){
  'use strict';

  const APP_VERSION = '20260731-1';
  const tg = window.Telegram?.WebApp;
  const root = document.documentElement;
  let scrollRoot = null;

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
        min-height:var(--tg-viewport-stable-height-js, 100dvh);
      }
      html.tg-miniapp-ready body{
        position:relative;
      }
      html.tg-miniapp-ready .miniapp-scroll-root{
        min-height:0;
        overflow-x:hidden;
        overflow-y:auto;
        overscroll-behavior-y:contain;
        -webkit-overflow-scrolling:touch;
        touch-action:pan-y;
      }
      html.tg-miniapp-ready .miniapp-scroll-root.is-scroll-locked{
        overflow:hidden;
      }
      html.tg-miniapp-ready .table-wrap{
        touch-action:pan-y;
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

  function installTouchGuard(){
    // Rely on Telegram.WebApp.disableVerticalSwipes() to avoid gesture conflicts.
    // Extra touchmove preventDefault handlers caused scroll regressions on some pages.
  }

  function findVerticalScrollTarget(element){
    let node = element?.parentElement || null;
    while (node && node !== document.body){
      const overflowY = window.getComputedStyle(node).overflowY;
      if (/auto|scroll/.test(overflowY) && node.scrollHeight > node.clientHeight + 1) return node;
      node = node.parentElement;
    }
    return document.scrollingElement || document.documentElement;
  }

  function installTableScrollHandoff(){
    let touch = null;

    document.addEventListener('wheel', (event) => {
      const table = event.target.closest?.('.table-wrap');
      if (!table || Math.abs(event.deltaY) <= Math.abs(event.deltaX)) return;

      const target = findVerticalScrollTarget(table);
      const before = target.scrollTop;
      target.scrollTop += event.deltaY;
      if (target.scrollTop !== before) event.preventDefault();
    }, { passive: false });

    document.addEventListener('touchstart', (event) => {
      const table = event.target.closest?.('.table-wrap');
      if (!table || event.touches.length !== 1) return;
      const point = event.touches[0];
      touch = {
        table,
        startX: point.clientX,
        startY: point.clientY,
        startScrollLeft: table.scrollLeft,
        axis: null
      };
    }, { passive: true });

    document.addEventListener('touchmove', (event) => {
      if (!touch || event.touches.length !== 1) return;
      const point = event.touches[0];
      const deltaX = point.clientX - touch.startX;
      const deltaY = point.clientY - touch.startY;

      if (!touch.axis && Math.max(Math.abs(deltaX), Math.abs(deltaY)) >= 8){
        touch.axis = Math.abs(deltaX) > Math.abs(deltaY) ? 'x' : 'y';
      }
      if (touch.axis !== 'x') return;

      const maxScrollLeft = Math.max(0, touch.table.scrollWidth - touch.table.clientWidth);
      touch.table.scrollLeft = Math.max(0, Math.min(maxScrollLeft, touch.startScrollLeft - deltaX));
      event.preventDefault();
    }, { passive: false });

    const clearTouch = () => { touch = null; };
    document.addEventListener('touchend', clearTouch, { passive: true });
    document.addEventListener('touchcancel', clearTouch, { passive: true });
  }

  function boot(){
    installBaseStyles();
    initTelegram();
    markScrollRoot();
    installTouchGuard();
    installTableScrollHandoff();
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
